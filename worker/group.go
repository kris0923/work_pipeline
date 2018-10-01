package worker

import "fmt"

type Group interface {
	Init() error
	Start() error
	Stop() error

	GetInQueue() Queue
	GetWorkerName() string
	GetWorkerNum() int
	GetWorker(int) Worker
	GetGroupType() GROUP_TYPE
	GetFollowQueues() []Queue

	PutNextPipe(interface{})
	AddFollowQueue(Queue) []Queue
	SetInQueue(Queue)
}

type WorkerFunc func(wg Group, wIdx int) Worker

func NewBaseGroup(
	wp Pipeline,
	workerName string,
	workerNum int,
	newWorker WorkerFunc,
	gType GROUP_TYPE,
	workerConfig interface{}) *BaseGroup {

	if 0 != workerNum&(workerNum-1) {
		panic("WorkerGroup' WorkerNum Must 2 4 8 16 ...")
	}

	return &BaseGroup{
		WorkerPipeline: wp,
		GroupType:      gType,
		WorkerName:     workerName,
		WorkerNum:      workerNum,
		InQueue:        NewQueue(QUEUE_CAP_ONE),
		FollowQueues:   make([]Queue, 0, NEXT_PIPE_NUM),
		Workers:        make([]Worker, workerNum),
		NewWorker:      newWorker,
		WorkerConfig:   workerConfig,
	}

}

type BaseGroup struct {
	WorkerPipeline Pipeline
	GroupType      GROUP_TYPE
	WorkerName     string
	WorkerNum      int
	InQueue        Queue
	FollowQueues   []Queue
	Workers        []Worker
	NewWorker      WorkerFunc
	WorkerConfig   interface{}
}

func (bg *BaseGroup) Init() error {
	fmt.Println(bg.GetWorkerName() + " Init")
	for i := 0; i < bg.WorkerNum; i++ {
		worker := bg.NewWorker(bg, i)
		worker.Init()
		bg.Workers[i] = worker
	}

	return nil
}
func (bg *BaseGroup) Start() error {
	fmt.Println(bg.GetWorkerName() + " Start")
	if DISPATCH_GROUP == bg.GetGroupType() {
		for _, worker := range bg.Workers {
			if err := worker.Dispatch(); nil != err {
				return err
			}
		}
	}
	for _, worker := range bg.Workers {
		if err := worker.Start(); nil != err {
			return err
		}
	}
	return nil
}
func (bg *BaseGroup) Stop() error {
	for _, worker := range bg.Workers {
		if err := worker.Stop(); nil != err {
			return err
		}
	}
	return nil
}

func (bg *BaseGroup) PutNextPipe(msg interface{}) {
	for _, queue := range bg.FollowQueues {
		queue.Put(msg)
	}

}

func (bg *BaseGroup) GetInQueue() Queue {
	return bg.InQueue
}

func (bg *BaseGroup) SetInQueue(inQueue Queue) {
	bg.InQueue = inQueue
}

func (bg *BaseGroup) GetFollowQueues() []Queue {
	return bg.FollowQueues
}
func (bg *BaseGroup) AddFollowQueue(outQueue Queue) []Queue {
	bg.FollowQueues = append(bg.FollowQueues, outQueue)
	return bg.FollowQueues

}

func (bg *BaseGroup) GetWorkerName() string {
	return bg.WorkerName
}
func (bg *BaseGroup) GetWorkerNum() int {
	return bg.WorkerNum
}
func (bg *BaseGroup) GetGroupType() GROUP_TYPE {
	return bg.GroupType
}

func (bg *BaseGroup) GetWorker(idx int) Worker {
	return bg.Workers[idx]
}
