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
