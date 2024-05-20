package network

type Semaphore struct {
	concurrency int
	channel     chan struct{}
}

func NewSemaphore(concurrency int) *Semaphore {
	return &Semaphore{
		concurrency: concurrency,
		channel:     make(chan struct{}, concurrency),
	}
}

func (s *Semaphore) Acquire() {
	s.channel <- struct{}{}
}

func (s *Semaphore) Release() {
	<-s.channel
}

func (s *Semaphore) IsFull() bool {
	return len(s.channel) >= s.concurrency
}

func (s *Semaphore) WithSemaphore(fn func()) {
	if fn == nil {
		return
	}
	s.Acquire()
	defer s.Release()
	fn()
}
