package api

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

func routeBucket(route string) []byte {
	if strings.HasPrefix(route, "turn-binding:") || strings.HasPrefix(route, "recovered:") || strings.HasPrefix(route, "prompt-cache:") {
		return []byte("turns")
	}
	return []byte("responses")
}

// 短生命週期檔案鎖與逐筆交易，不在程序結束或更新時留下待寫資料。
func (h *HTTPAPI) withTurnDB(action func(*bolt.DB) error) error {
	path := filepath.Join(filepath.Dir(h.turnBindingPath()), "provider_routes.db")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()
	ready := false
	if err = db.View(func(tx *bolt.Tx) error { ready = tx.Bucket([]byte("legacy")) != nil; return nil }); err != nil {
		return err
	}
	if !ready {
		entries, err := h.readTurnBindings()
		if err != nil {
			return err
		}
		err = db.Update(func(tx *bolt.Tx) error {
			for _, name := range []string{"turns", "responses", "legacy", "metadata"} {
				if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
					return err
				}
			}
			b := tx.Bucket([]byte("legacy"))
			for key, entry := range entries {
				data, err := json.Marshal(entry)
				if err != nil {
					return err
				}
				if err = b.Put([]byte(key), data); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return action(db)
}

func readRouteEntry(tx *bolt.Tx, bucket []byte, key string) (savedTurnBinding, bool, error) {
	var entry savedTurnBinding
	data := tx.Bucket(bucket).Get([]byte(key))
	if data == nil {
		data = tx.Bucket([]byte("legacy")).Get([]byte(key))
	}
	if data == nil {
		return entry, false, nil
	}
	if err := json.Unmarshal(data, &entry); err != nil {
		return entry, false, err
	}
	return entry, entry.Until.After(time.Now()), nil
}

func putRouteEntry(tx *bolt.Tx, bucket []byte, key string, entry savedTurnBinding) error {
	b := tx.Bucket(bucket)
	limit := uint64(100000)
	if string(bucket) == "responses" {
		limit = 500000
	}
	if b.Get([]byte(key)) == nil {
		count := b.Sequence()
		if count >= limit {
			return fmt.Errorf("%s 配對儲存已達容量上限", bucket)
		}
		if count >= limit*8/10 && count%1000 == 0 {
			log.Printf("route storage nearing capacity: bucket=%s entries=%d limit=%d", bucket, count, limit)
		}
		if err := b.SetSequence(count + 1); err != nil {
			return err
		}
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if err = b.Put([]byte(key), data); err != nil {
		return err
	}
	return tx.Bucket([]byte("legacy")).Delete([]byte(key))
}

func pruneTurnDB(tx *bolt.Tx) error {
	meta := tx.Bucket([]byte("metadata"))
	last, _ := time.Parse(time.RFC3339, string(meta.Get([]byte("pruned"))))
	if time.Since(last) < time.Hour {
		return nil
	}
	for _, name := range []string{"turns", "responses", "legacy"} {
		b := tx.Bucket([]byte(name))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var entry savedTurnBinding
			if err := json.Unmarshal(v, &entry); err != nil {
				return err
			}
			if !entry.Until.After(time.Now()) {
				if err := c.Delete(); err != nil {
					return err
				}
				if b.Sequence() > 0 {
					if err := b.SetSequence(b.Sequence() - 1); err != nil {
						return err
					}
				}
			}
		}
	}
	return meta.Put([]byte("pruned"), []byte(time.Now().Format(time.RFC3339)))
}
