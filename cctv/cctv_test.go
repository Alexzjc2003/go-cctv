package cctv_test

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alexzjc2003/go-cctv/cctv"
)

func TestPlatform(t *testing.T) {
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
			slog.Info("Got content", "content", i)
		}
	})

	wg.Wait()
}

func TestUnsubscription(t *testing.T) {
	p := cctv.NewPlatform()

	var wg sync.WaitGroup
	wg.Go(func() {
		i := 0
		for range time.Tick(1 * time.Second) {
			i += 1
			p.Publish("", i)
			if i == 10 {
				break
			}
		}

	})

	wg.Add(5)
	for i := range 5 {
		go func(i int) {
			defer wg.Done()
			s, ch := p.SubscribeWithConfig("", cctv.SubConfig{})

			time.AfterFunc((2+time.Duration(i))*time.Second, s.Unsubscribe)
			for c := range ch {
				slog.Info("Got content", "sub", i, "content", c)
			}
		}(i)
	}
	wg.Wait()
}
