package queue

import (
	"pbft/pbft"
	"sync"
)

// Queue represents a thread-safe FIFO queue for TransactionRequests
type Queue struct {
	items []*pbft.TransactionRequest
	mu    sync.Mutex
	cond  *sync.Cond
}

// NewQueue initializes and returns a new Queue
func NewQueue() *Queue {
	q := &Queue{
		items: make([]*pbft.TransactionRequest, 0),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Enqueue adds a TransactionRequest to the queue
func (q *Queue) Enqueue(item *pbft.TransactionRequest) {
	q.mu.Lock()
	q.items = append(q.items, item)
	q.mu.Unlock()
	q.cond.Signal() // Notify one waiting goroutine
}

// Dequeue removes and returns the first TransactionRequest from the queue
// If the queue is empty, it blocks until an item is available
func (q *Queue) Dequeue() *pbft.TransactionRequest {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.items) == 0 {
		q.cond.Wait()
	}

	item := q.items[0]
	q.items = q.items[1:]
	return item
}

// Size returns the current number of items in the queue
func (q *Queue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}
