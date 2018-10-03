package worker

//Sometime chan is not the best choice
//change Queue defination just OK
type Queue chan interface{}

func NewQueue(size int) Queue {
	return make(Queue, size)
}

func (q Queue) Put(msg interface{}) {
	q <- msg
}

func (q Queue) Get() interface{} {
	return <-q
}
func (q Queue) Close() {
	close(q)
}
