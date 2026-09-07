package api

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

func newRequestDiagnosticID() string {
	var id [16]byte
	if _, err := rand.Read(id[:]); err == nil {
		return hex.EncodeToString(id[:])
	}
	return fmt.Sprintf("local-%d", time.Now().UnixNano())
}

// 只接受識別碼字元，不將任意上游標頭內容寫入 Log。
func diagnosticHeaderID(value string) string {
	if len(value) > 128 {
		return "invalid-id"
	}
	for _, ch := range value {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' {
			continue
		}
		return "invalid-id"
	}
	return value
}
