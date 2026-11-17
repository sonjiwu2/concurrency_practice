package ovens

import (
	"context"
	"sync"
	"time"
)

// Задание 1: общая map и Mutex. Каркас ожидания уже дан.
func CheckAllOvens(items []OvenTask, work OvenChecker) []OvenStatus {
	results := make(map[int]OvenStatus)
	var wg sync.WaitGroup
	var mtx sync.Mutex
	// TODO: объявите общий мьютекс для results.
	if len(items) == 0 {
		return nil
	}
	for index, item := range items {
		wg.Add(1)
		go func(index int, item OvenTask) {
			defer wg.Done()
			// TODO: вызовите work вне блокировки и сохраните результат под мьютексом.
			workResult := work(item)
			mtx.Lock()
			results[index] = workResult
			mtx.Unlock()
		}(index, item)
	}
	wg.Wait()
	output := make([]OvenStatus, len(items))
	for index := range items {
		output[index] = results[index]
	}
	return output
}

// Задание 2: Fan-out / Fan-in. Каркас объединения потоков уже дан.
func FanOut(in <-chan OvenTask, workers int, work OvenChecker) []<-chan OvenStatus {
	if workers <= 0 {
		return nil
	}

	result := make([]<-chan OvenStatus, workers)

	for i := range result {
		out := make(chan OvenStatus)
		result[i] = out

		go func(out chan<- OvenStatus) {
			defer close(out)

			for item := range in {
				out <- work(item)
			}
		}(out)
	}

	return result
}

func FanIn(streams ...<-chan OvenStatus) <-chan OvenStatus {
	out := make(chan OvenStatus)
	var wg sync.WaitGroup

	for _, stream := range streams {
		wg.Add(1)

		go func(stream <-chan OvenStatus) {
			defer wg.Done()

			for msg := range stream {
				out <- msg
			}
		}(stream)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func WaitForOven(ctx context.Context, ready <-chan OvenStatus, timeout time.Duration) (OvenStatus, error) {
	if timeout <= 0 {
		return OvenStatus{}, ErrInvalidTimeout
	}

	if err := ctx.Err(); err != nil {
		return OvenStatus{}, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case msg, ok := <-ready:
		if !ok {
			return OvenStatus{}, ErrClosed
		}
		return msg, nil

	case <-timer.C:
		return OvenStatus{}, ErrTimeout

	case <-ctx.Done():
		return OvenStatus{}, ctx.Err()
	}
}
