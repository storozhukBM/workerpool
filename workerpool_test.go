package workerpool_test

import (
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cristalhq/workerpool"
)

func TestWorkerPool(t *testing.T) {
	const statsRefreshPeriod = 250 * time.Millisecond
	const shortDelay = 50 * time.Millisecond
	const workers = 10
	wp := workerpool.NewWithOptions(0, workers, statsRefreshPeriod)
	eq(t, 0, wp.WorkersInPool())
	eq(t, 0, wp.CurrentRunningWorkers())
	eq(t, workers, wp.TargetWorkersInPool())
	eq(t, 0, wp.MaxConcurrencyObservedInCycle())
	eq(t, 0, wp.AdaptationsExecuted())

	startG := runtime.NumGoroutine()
	jobsDone := int64(0)

	// submit tasks and grow till `workers` goroutines
	// create twice more workers than expected
	for i := 1; i <= 2*workers; i++ {
		wp.Do(func() {
			time.Sleep(shortDelay)
			atomic.AddInt64(&jobsDone, 1)
		})
		waitUntil(t, func() bool { return wp.CurrentRunningWorkers() == i }, shortDelay/workers)
		eq(t, 0, wp.WorkersInPool())
		eq(t, i, wp.CurrentRunningWorkers())
		eq(t, workers, wp.TargetWorkersInPool())
		eq(t, i, wp.MaxConcurrencyObservedInCycle())
		eq(t, 0, wp.AdaptationsExecuted())
		eq(t, i, runtime.NumGoroutine()-startG)
		eq(t, int64(0), atomic.LoadInt64(&jobsDone))
	}

	// wait for tasks to finish and check that only max waiting workers are still waiting for new tasks
	waitUntil(t, func() bool { return atomic.LoadInt64(&jobsDone) == 2*workers }, 2*shortDelay)
	eq(t, workers, wp.WorkersInPool())
	eq(t, 0, wp.CurrentRunningWorkers())
	eq(t, workers, wp.TargetWorkersInPool())
	eq(t, 2*workers, wp.MaxConcurrencyObservedInCycle())
	eq(t, 0, wp.AdaptationsExecuted())
	eq(t, workers, runtime.NumGoroutine()-startG)
	eq(t, int64(2*workers), atomic.LoadInt64(&jobsDone))

	// wait for the first adaption cycle and check that target workers in pool remains
	waitUntil(t, func() bool { return wp.AdaptationsExecuted() == 1 }, statsRefreshPeriod)
	eq(t, workers, wp.WorkersInPool())
	eq(t, 0, wp.CurrentRunningWorkers())
	eq(t, workers, wp.TargetWorkersInPool())
	eq(t, 0, wp.MaxConcurrencyObservedInCycle())
	eq(t, 1, wp.AdaptationsExecuted())
	eq(t, workers, runtime.NumGoroutine()-startG)
	eq(t, int64(2*workers), atomic.LoadInt64(&jobsDone))

	// run less tasks
	for i := 1; i <= workers/2; i++ {
		wp.Do(func() {
			time.Sleep(shortDelay)
			atomic.AddInt64(&jobsDone, 1)
		})
		waitUntil(t, func() bool { return wp.CurrentRunningWorkers() == i }, shortDelay/workers)
		eq(t, workers, wp.WorkersInPool())
		eq(t, i, wp.CurrentRunningWorkers())
		eq(t, workers, wp.TargetWorkersInPool())
		eq(t, i, wp.MaxConcurrencyObservedInCycle())
		eq(t, 1, wp.AdaptationsExecuted())
		eq(t, workers, runtime.NumGoroutine()-startG)
		eq(t, int64(2*workers), atomic.LoadInt64(&jobsDone))
	}

	// wait for tasks to finish and check that only target waiting workers are still waiting for new tasks
	waitUntil(t, func() bool { return atomic.LoadInt64(&jobsDone) == 2*workers+(workers/2) }, 2*shortDelay)
	eq(t, workers, wp.WorkersInPool())
	eq(t, 0, wp.CurrentRunningWorkers())
	eq(t, workers, wp.TargetWorkersInPool())
	eq(t, workers/2, wp.MaxConcurrencyObservedInCycle())
	eq(t, 1, wp.AdaptationsExecuted())
	eq(t, workers, runtime.NumGoroutine()-startG)
	eq(t, int64(2*workers+(workers/2)), atomic.LoadInt64(&jobsDone))

	// wait for tasks to finish and check that adaptation happens
	waitUntil(t, func() bool { return wp.AdaptationsExecuted() == 2 }, statsRefreshPeriod)
	{
		previousTarget := workers
		maxConcurrencyOnThisCycle := workers / 2
		middlePointBetweenPrevTargetAndNewOne := (previousTarget + maxConcurrencyOnThisCycle) / 2
		newTarget := middlePointBetweenPrevTargetAndNewOne
		// check that target workers drop to new target
		waitUntil(t, func() bool { return wp.WorkersInPool() == newTarget }, 2*shortDelay)
		eq(t, 0, wp.CurrentRunningWorkers())
		eq(t, newTarget, wp.TargetWorkersInPool())
		eq(t, 0, wp.MaxConcurrencyObservedInCycle())
		eq(t, newTarget, runtime.NumGoroutine()-startG)
		eq(t, 2, wp.AdaptationsExecuted())
		eq(t, int64(2*workers+(workers/2)), atomic.LoadInt64(&jobsDone))
	}

	// wait for tasks to finish and check that adaptation continues
	waitUntil(t, func() bool { return wp.AdaptationsExecuted() == 3 }, statsRefreshPeriod)
	{
		previousTarget := (workers + workers/2) / 2
		maxConcurrencyOnThisCycle := 0
		middlePointBetweenPrevTargetAndNewOne := (previousTarget + maxConcurrencyOnThisCycle) / 2
		newTarget := middlePointBetweenPrevTargetAndNewOne
		// check that target workers drop to new target
		waitUntil(t, func() bool { return wp.WorkersInPool() == newTarget }, 2*shortDelay)
		eq(t, 0, wp.CurrentRunningWorkers())
		eq(t, newTarget, wp.TargetWorkersInPool())
		eq(t, 0, wp.MaxConcurrencyObservedInCycle())
		eq(t, newTarget, runtime.NumGoroutine()-startG)
		eq(t, 3, wp.AdaptationsExecuted())
		eq(t, int64(2*workers+(workers/2)), atomic.LoadInt64(&jobsDone))
	}

	// wait for pool to release all workers
	waitUntil(t, func() bool { return wp.TargetWorkersInPool() == 0 }, 3*statsRefreshPeriod)
	eq(t, 0, wp.WorkersInPool())
	eq(t, 0, wp.CurrentRunningWorkers())
	eq(t, 0, wp.TargetWorkersInPool())
	eq(t, 0, wp.MaxConcurrencyObservedInCycle())
	eq(t, 0, runtime.NumGoroutine()-startG)
	eq(t, int64(2*workers+(workers/2)), atomic.LoadInt64(&jobsDone))

	lastCycleNumber := wp.AdaptationsExecuted()

	// submit just one task
	wp.Do(func() {
		time.Sleep(shortDelay)
		atomic.AddInt64(&jobsDone, 1)
	})
	waitUntil(t, func() bool { return wp.CurrentRunningWorkers() == 1 }, shortDelay/10)
	eq(t, 0, wp.WorkersInPool())
	eq(t, 1, wp.CurrentRunningWorkers())
	eq(t, 0, wp.TargetWorkersInPool())
	eq(t, 1, wp.MaxConcurrencyObservedInCycle())
	eq(t, 1, runtime.NumGoroutine()-startG)
	eq(t, int64(2*workers+(workers/2)), atomic.LoadInt64(&jobsDone))

	// check that task was finished
	waitUntil(t, func() bool { return atomic.LoadInt64(&jobsDone) == int64(2*workers+(workers/2))+1 }, 2*shortDelay)
	eq(t, 0, wp.WorkersInPool())
	eq(t, 0, wp.CurrentRunningWorkers())
	eq(t, 0, wp.TargetWorkersInPool())
	eq(t, 1, wp.MaxConcurrencyObservedInCycle())
	eq(t, 0, runtime.NumGoroutine()-startG)
	eq(t, int64(2*workers+(workers/2))+1, atomic.LoadInt64(&jobsDone))

	// check that adaptation is not running when pool is empty and there is no tasks
	time.Sleep(2 * statsRefreshPeriod)
	eq(t, 0, wp.WorkersInPool())
	eq(t, 0, wp.CurrentRunningWorkers())
	eq(t, 0, wp.TargetWorkersInPool())
	eq(t, 1, wp.MaxConcurrencyObservedInCycle())
	eq(t, lastCycleNumber, wp.AdaptationsExecuted())
	eq(t, 0, runtime.NumGoroutine()-startG)
	eq(t, int64(2*workers+(workers/2)+1), atomic.LoadInt64(&jobsDone))

	// submit one more task and check that adaptation happens immediately after work is finished
	wp.Do(func() {
		time.Sleep(shortDelay)
		atomic.AddInt64(&jobsDone, 1)
	})
	waitUntil(t, func() bool { return wp.CurrentRunningWorkers() == 1 }, shortDelay/10)
	eq(t, 0, wp.WorkersInPool())
	eq(t, 1, wp.CurrentRunningWorkers())
	eq(t, 0, wp.TargetWorkersInPool())
	eq(t, 1, wp.MaxConcurrencyObservedInCycle())
	eq(t, lastCycleNumber, wp.AdaptationsExecuted())
	eq(t, 1, runtime.NumGoroutine()-startG)
	eq(t, int64(2*workers+(workers/2)+1), atomic.LoadInt64(&jobsDone))

	waitUntil(t, func() bool { return atomic.LoadInt64(&jobsDone) == int64(2*workers+(workers/2))+2 }, 2*shortDelay)
	eq(t, 1, wp.WorkersInPool())
	eq(t, 0, wp.CurrentRunningWorkers())
	eq(t, 1, wp.TargetWorkersInPool())
	eq(t, 0, wp.MaxConcurrencyObservedInCycle())
	eq(t, lastCycleNumber+1, wp.AdaptationsExecuted())
	eq(t, 1, runtime.NumGoroutine()-startG)
	eq(t, int64(2*workers+(workers/2))+2, atomic.LoadInt64(&jobsDone))
}

func TestVisualisationExponentialDeclineOfWaitingTime(t *testing.T) {
	t.Skip() // this test created only for local visualisation purposes
	wp := workerpool.NewWithOptions(5, 128, 10*time.Millisecond)
	go func() {
		for {
			wp.Do(func() {
				time.Sleep(1 * time.Millisecond)
			})
			verticalShift := 20_000.0
			amplitude := 17_000.0
			periodLength := 75.0
			d := time.Duration(
				verticalShift +
					amplitude*
						math.Sin(
							float64(time.Now().UnixMilli())*(2*math.Pi/periodLength),
						),
			)
			time.Sleep(d)
		}
	}()
	for i := 0; i < 1000; i++ {
		bar := strings.Repeat("*", wp.CurrentRunningWorkers())
		workersWaiting := wp.WorkersInPool() - wp.CurrentRunningWorkers()
		if workersWaiting > 0 {
			bar += strings.Repeat("-", workersWaiting)
		}
		fmt.Printf(
			"%.4d ms: [%.4d/%.4d] (%.4d|%.4d) %s\n",
			i, wp.CurrentRunningWorkers(), wp.WorkersInPool(), wp.MaxConcurrencyObservedInCycle(), wp.TargetWorkersInPool(),
			bar,
		)
		time.Sleep(time.Millisecond)
	}
}

func TestWorkerParamsValidation(t *testing.T) {
	workerpool.NewWithOptions(0, 1, time.Millisecond)
	workerpool.NewWithOptions(math.MaxInt32, math.MaxInt32, math.MaxInt64*time.Nanosecond)

	expectPanic(t, func() {
		workerpool.NewWithOptions(0, 0, time.Second)
	}, "workerpool: maxWorkersInPool should be between 1 and math.MaxInt32")
	expectPanic(t, func() {
		workerpool.NewWithOptions(0, -1, time.Second)
	}, "workerpool: maxWorkersInPool should be between 1 and math.MaxInt32")
	expectPanic(t, func() {
		workerpool.NewWithOptions(0, int(math.MaxInt32)+1, time.Second)
	}, "workerpool: maxWorkersInPool should be between 1 and math.MaxInt32")

	expectPanic(t, func() {
		workerpool.NewWithOptions(-1, 1, time.Second)
	}, "workerpool: minWorkersInPool should be between 0 and math.MaxInt32 and not bigger than maxWorkersInPool")
	expectPanic(t, func() {
		workerpool.NewWithOptions(int(math.MaxInt32)+1, 1, time.Second)
	}, "workerpool: minWorkersInPool should be between 0 and math.MaxInt32 and not bigger than maxWorkersInPool")
	expectPanic(t, func() {
		workerpool.NewWithOptions(10, 9, time.Second)
	}, "workerpool: minWorkersInPool should be between 0 and math.MaxInt32 and not bigger than maxWorkersInPool")

	expectPanic(t, func() {
		workerpool.NewWithOptions(0, 1, time.Millisecond-time.Nanosecond)
	}, "workerpool: adaptationPeriod should be at least 1 millisecond")
	expectPanic(t, func() {
		workerpool.NewWithOptions(0, 1, time.Duration(0))
	}, "workerpool: adaptationPeriod should be at least 1 millisecond")
}

func expectPanic(t *testing.T, f func(), expectedError string) {
	t.Helper()
	var actualPanic interface{}
	func() {
		defer func() {
			actualPanic = recover()
		}()
		f()
	}()
	if actualPanic == nil {
		t.Fatal("panic isn't detected")
	}
	if actualPanic.(string) != expectedError {
		t.Fatalf("unnexpected panic message: %v", actualPanic)
	}
}

func waitUntil(t *testing.T, check func() bool, atMost time.Duration) {
	t.Helper()
	deadline := time.Now().Add(atMost)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(atMost / 100)
	}
	t.Fatalf("time limit reached")
}

func eq(t *testing.T, expected interface{}, actual interface{}) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("\nexp: %T:`%#v`\nact: %T:`%#v`", expected, expected, actual, actual)
	}
}
