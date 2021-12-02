`WorkerPool` allows you to execute any function in a separate goroutine
it is designed to act exactly like `go` operator, with the only difference that
goroutines after they finish the task, will stay for a bit and wait for additional tasks.

This instrument allows you to reduce the overhead of creating new goroutines
if your tasks are so small that such overhead can become noticeable.

Another scenario where `WorkerPool` can help is when your typical task triggers goroutine stack growth,
and you want to avoid creation and stack expansion repeated over and over.

This `WorkerPool` is also adaptive. It tracks how many concurrent tasks it runs during adaptation period
and tries to keep at least that amount of workers ready within the next adaptation period.
