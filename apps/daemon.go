package main

import (
	"chaossir/work_pipeline/guid"
	"chaossir/work_pipeline/worker"
	"fmt"
	"strconv"
)

func main() {
	pl := worker.NewBasePipeline()

	wg1 := worker.NewBaseGroup(
		pl,
		"start",
		4,
		NewWorkerFunc,
		worker.NORMAL_GROUP,
		nil,
	)
	pl.Insert(wg1)

	b1 := worker.NewBaseGroup(
		pl,
		"dispatch",
		4,
		NewWorkerFunc,
		worker.DISPATCH_GROUP,
		nil,
	)
	pl.Insert(b1, "start")

	ew := worker.NewBaseGroup(
		pl,
		"end",
		4,
		NewWorkerFunc,
		worker.NORMAL_GROUP,
		nil,
	)
	pl.Insert(ew, "dispatch")
	pl.Init()
	pl.Start()
	quit := make(chan bool)

	for {
		select {
		case <-quit:
			break
		}
	}
}

func NewWorkerFunc(wg worker.Group, wIdx int) worker.Worker {
	switch wg.GetWorkerName() {
	case "start":
		//fmt.Println("NewWorkerFunc start InitFunc")
		return &A1Worker{
			BaseWorker: *worker.NewBaseWorker(wg, wIdx),
		}
	case "dispatch":
		return &B1Worker{
			BaseWorker: *worker.NewBaseWorker(wg, wIdx),
		}
	case "end":
		//fmt.Println("NewWorkerFunc end InitFunc")
		return &EndWorker{
			BaseWorker: *worker.NewBaseWorker(wg, wIdx),
		}
	default:
		return nil
	}
}

type Msg struct {
	ID  guid.GUID
	Idx int
}

type A1Worker struct {
	worker.BaseWorker
}

func (aw *A1Worker) Init() error {
	fmt.Println("AW " + aw.WorkerGroup.GetWorkerName() + "worker[ " + strconv.Itoa(aw.WorkerIdx) + "] Init")
	return nil

}

func (aw *A1Worker) Start() error {
	fmt.Println("AW " + aw.WorkerGroup.GetWorkerName() + "worker[ " + strconv.Itoa(aw.WorkerIdx) + "] Start")
	guidFactory := guid.NewGUIDFactory(int64(aw.WorkerIdx))
	go func() {
		for idx := 0; idx < 1000000; idx++ {
			curGuid, _ := guidFactory.NewGUID()
			fmt.Println("AW " + aw.WorkerGroup.GetWorkerName() + "worker[ " + strconv.Itoa(aw.WorkerIdx) + "] Generate Msg {ID:" + strconv.FormatInt(int64(curGuid), 10) + ",Idx:" + strconv.Itoa(idx) + "}")
			aw.WorkerGroup.PutNextPipe(&Msg{ID: curGuid, Idx: idx})
		}
		fmt.Println("AW " + aw.WorkerGroup.GetWorkerName() + "worker[ " + strconv.Itoa(aw.WorkerIdx) + "] End")

	}()

	return nil
}

type B1Worker struct {
	worker.BaseWorker
}

func (bw *B1Worker) process(msgIn interface{}) (interface{}, error) {
	msg := msgIn.(*Msg)
	fmt.Println("Msg{ID:" + strconv.FormatInt(int64(msg.ID), 10) + ",Idx:" + strconv.Itoa(msg.Idx) + " Is Processing By B1Worker [" + strconv.Itoa(bw.WorkerIdx) + "]")
	return msgIn, nil
}

func (bw *B1Worker) Start() error {
	fmt.Println("BW " + bw.WorkerGroup.GetWorkerName() + "worker[ " + strconv.Itoa(bw.WorkerIdx) + "] Start")
	go func() {
		for {
			msgIn := bw.InQueue.Get()
			if nil == msgIn {
				break
			}
			if msgOut, err := bw.process(msgIn); nil == err {
				//TODO Error Process
				bw.WorkerGroup.PutNextPipe(msgOut)
			}
		}
		fmt.Println("BW " + bw.WorkerGroup.GetWorkerName() + "worker[ " + strconv.Itoa(bw.WorkerIdx) + "] End")
	}()
	return nil
}

func (bw *B1Worker) Dispatch() error {
	fmt.Println("BW " + bw.WorkerGroup.GetWorkerName() + "worker[ " + strconv.Itoa(bw.WorkerIdx) + "] StartDispatch")
	go func() {
		workerNum := bw.WorkerGroup.GetWorkerNum()
		idx := 0
		for {
			msgIn := bw.WorkerGroup.GetInQueue().Get()
			if nil == msgIn {
				break
			}

			msg := msgIn.(*Msg)
			idx = msg.Idx & (workerNum - 1)
			fmt.Println("Msg{ID:" + strconv.FormatInt(int64(msg.ID), 10) + ",Idx:" + strconv.Itoa(msg.Idx) + " Is Dispatch to B1Worker [" + strconv.Itoa(idx) + "]")
			bw.WorkerGroup.GetWorker(idx).GetInQueue().Put(msgIn)
			//idx = (idx + 1) & (workerNum - 1)
		}
	}()
	return nil
}

type EndWorker struct {
	worker.BaseWorker
}

func (ew *EndWorker) Init() error {
	fmt.Println("EW " + ew.WorkerGroup.GetWorkerName() + "worker[ " + strconv.Itoa(ew.WorkerIdx) + "] Init")
	return nil
}

func (ew *EndWorker) Start() error {
	fmt.Println("EW " + ew.WorkerGroup.GetWorkerName() + "worker[ " + strconv.Itoa(ew.WorkerIdx) + "] Start")
	if worker.DISPATCH_GROUP == ew.WorkerGroup.GetGroupType() {
		ew.Dispatch()
	}
	go func() {
		for {
			msgIn := ew.InQueue.Get()
			if nil == msgIn {
				break
			}
			if _, err := ew.process(msgIn); nil != err {
				//TODO Error Process
			}
		}
		fmt.Println("EW " + ew.WorkerGroup.GetWorkerName() + "worker[ " + strconv.Itoa(ew.WorkerIdx) + "] End")
	}()
	return nil
}

func (ew *EndWorker) process(msgIn interface{}) (interface{}, error) {
	msg := msgIn.(*Msg)
	fmt.Println("Msg{ID:" + strconv.FormatInt(int64(msg.ID), 10) + ",Idx:" + strconv.Itoa(msg.Idx) + " Is Processing By EndWorker [" + strconv.Itoa(ew.WorkerIdx) + "]")
	return nil, nil
}
