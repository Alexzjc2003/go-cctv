package cctv_test

import (
	"cctv"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func TestPlatform(t *testing.T) {
	slog.Info("Hello, Test")

	p := cctv.NewPlatform()
	s, ch := p.SubscribeWithConfig("", cctv.SubConfig{})

	var wg sync.WaitGroup
	wg.Go(func() {
		i := 0
		for range time.Tick(1 * time.Second) {
			i += 1
			p.Publish("", i)
			if i == 5 {
				break
			}
		}

		s.Unsubscribe()
	})
	wg.Go(func() {
		for i := range ch {
			slog.Info("Got content", i)
		}
	})

	wg.Wait()
}
