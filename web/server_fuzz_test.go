package web

import "testing"

func FuzzPayloadStringNeverPanics(f *testing.F) {
	f.Add("worker")
	f.Add("")
	f.Fuzz(func(t *testing.T, value string) {
		_, _ = payloadString(map[string]interface{}{"name": value}, "name", 128)
	})
}

func FuzzPayloadIntNeverPanics(f *testing.F) {
	f.Add(float64(1))
	f.Add(float64(-1))
	f.Fuzz(func(t *testing.T, value float64) {
		_, _ = payloadInt(map[string]interface{}{"value": value}, "value", -100, 100)
	})
}

func FuzzIsLoopbackAddress(f *testing.F) {
	f.Add("127.0.0.1:8080")
	f.Add("localhost:9090")
	f.Add("[::1]:8000")
	f.Add("0.0.0.0:80")
	f.Add("192.168.1.1:3000")
	f.Fuzz(func(t *testing.T, addr string) {
		_ = IsLoopbackAddress(addr)
	})
}
