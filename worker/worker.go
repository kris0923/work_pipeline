package worker

import "fmt"
import "strconv"

type Worker interface {
	Init() error
	Start() error
	Stop() error
	Dispatch() error

	//Process(interface{}) (interface{}, error)

	SetInQueue(Queue)

	GetInQueue() Queue
}

func NewBaseWorker(wg Group, wIdx int) *BaseWorker {
	if nil == wg || wIdx < 0 {
		panic("NewBaseWorker Func Error")
	}
	inQueue := wg.GetInQueue()
	if DISPATCH_GROUP == wg.GetGroupType() {
		inQueue = NewQueue(QUEUE_CAP_ONE)
	}
	return &BaseWorker{
		WorkerGroup: wg,
		WorkerIdx:   wIdx,
		InQueue:     inQueue,
	}
}

type BaseWorker struct {
	WorkerGroup Group
	WorkerIdx   int
	InQueue     Queue
}

func (bw *BaseWorker) Init() error {
	fmt.Println("BW " + bw.WorkerGroup.GetWorkerName() + "worker[ " + strconv.Itoa(bw.WorkerIdx) + "] Init")
	return nil
}

func (bw *BaseWorker) Start() error {
	fmt.Println("BW " + bw.WorkerGroup.GetWorkerName() + "worker[ " + strconv.Itoa(bw.WorkerIdx) + "] Start")
	go func() {
		for {
			msgIn := bw.InQueue.Get()
			if nil == msgIn {
				break
			}
			if msgOut, err := bw.process(msgIn); nil == err {
				if nil != msgOut {
					bw.WorkerGroup.PutNextPipe(msgOut)
				}
			} else {
				//TODO Error Process
			}
		}
	}()
	return nil
}

func (bw *BaseWorker) Stop() error {
	return nil
}

func (bw *BaseWorker) process(msgIn interface{}) (interface{}, error) {
	fmt.Println("BaseWorker Process")
	return msgIn, nil
}

func (bw *BaseWorker) Dispatch() error {
	go func() {
		workerNum := bw.WorkerGroup.GetWorkerNum()
		idx := 0

		for {
			msgIn := bw.WorkerGroup.GetInQueue().Get()
			if nil == msgIn {
				break
			}
			bw.WorkerGroup.GetWorker(idx).GetInQueue().Put(msgIn)
			idx = (idx + 1) & (workerNum - 1)
		}
	}()
	return nil
}

func (bw *BaseWorker) GetInQueue() Queue {
	return bw.InQueue
}

func (bw *BaseWorker) SetInQueue(inQueue Queue) {
	bw.InQueue = inQueue
}
