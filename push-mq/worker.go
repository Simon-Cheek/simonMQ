package main

func (q *Queue) RunQueueWorker() {
	for {
		for {
			msg := q.Pop()
			if msg == nil {
				break
			}
			// process msg (later)
		}

		select {
		case <-q.hasWork:
			// loop back, go pop again
		case <-q.isClosed:
			return
		}
	}
}
