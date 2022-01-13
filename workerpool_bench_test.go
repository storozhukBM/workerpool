package workerpool_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cristalhq/workerpool"
)

var poolOfCounters = sync.Pool{New: func() interface{} {
	counter := uint64(0)
	return &counter
}}

func counterInc() {
	counter := poolOfCounters.Get().(*uint64)
	*counter++
	poolOfCounters.Put(counter)
}

func counterTryToReadSome() uint64 {
	result := uint64(0)
	for i := 0; i < 20000; i++ {
		counter := poolOfCounters.Get().(*uint64)
		result += *counter
	}
	return result
}

func actualTask(size int, recursion int) {
	if recursion < 200 {
		actualTask(size, recursion+1)
		return
	}
	time.Sleep(time.Duration(size))
	counterInc()
}

func taskBySize(size int) func() {
	return func() {
		actualTask(size, 0)
	}
}

func BenchmarkPoolOverhead(b *testing.B) {
	taskSizes := []int{1, 10, 100, 500, 1000, 5000, 10_000, 50_000, 100_000, 500_000, 1_000_000}
	for _, size := range taskSizes {
		b.Run(fmt.Sprintf("r-1_s-%v", size), func(b *testing.B) {
			b.ReportMetric(float64(size), "size")
			task := taskBySize(size)
			for i := 0; i < b.N; i++ {
				task()
			}
		})
	}
	if rand.Float64() < 0.00001 {
		fmt.Println(counterTryToReadSome())
	}
	for _, size := range taskSizes {
		b.Run(fmt.Sprintf("r-n_s-%v", size), func(b *testing.B) {
			b.ReportMetric(float64(size), "size")
			task := taskBySize(size)
			for i := 0; i < b.N; i++ {
				go task()
			}
		})
	}
	if rand.Float64() < 0.00001 {
		fmt.Println(counterTryToReadSome())
	}
	for _, size := range taskSizes {
		b.Run(fmt.Sprintf("r-w_s-%v", size), func(b *testing.B) {
			b.ReportMetric(float64(size), "size")
			task := taskBySize(size)
			wp := workerpool.NewWithOptions(1000, 100000, time.Millisecond)
			for i := 0; i < b.N; i++ {
				wp.Do(task)
			}
		})
	}
	if rand.Float64() < 0.00001 {
		fmt.Println(counterTryToReadSome())
	}
}
