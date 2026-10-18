package main

import (
	"encoding/json"
	"os"
	"time"
)

func main() {
	started := time.Now()
	var event json.RawMessage
	if err := json.NewDecoder(os.Stdin).Decode(&event); err != nil || !json.Valid(event) {
		os.Exit(2)
	}
	var control struct {
		DelayMS int `json:"delay_ms"`
	}
	if err := json.Unmarshal(event, &control); err != nil || control.DelayMS < 0 || control.DelayMS > 30_000 {
		os.Exit(2)
	}
	if control.DelayMS > 0 {
		time.Sleep(time.Duration(control.DelayMS) * time.Millisecond)
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"result":              event,
		"handler_duration_ms": time.Since(started).Milliseconds(),
	})
}
