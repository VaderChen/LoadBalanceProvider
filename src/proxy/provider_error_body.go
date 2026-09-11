package proxy

// 擷取錯誤摘要但不截斷轉送；大型錯誤本文也不會無限占用記憶體。
type providerErrorBodyCapture struct {
	body []byte
}

func (c *providerErrorBodyCapture) Write(p []byte) (int, error) {
	const limit = 64 * 1024
	n := min(len(p), limit-len(c.body))
	c.body = append(c.body, p[:n]...)
	return len(p), nil
}
