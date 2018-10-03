package main

import (
	"chaossir/work_pipeline/guid"
	"chaossir/work_pipeline/worker"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strconv"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6062", nil))
	}()
	pl := worker.NewBasePipeline()

	wg1 := worker.NewBaseGroup(
		pl,
		"start",
		1<<4,
		NewWorkerFunc,
		worker.NORMAL_GROUP,
		nil,
	)
	pl.Insert(wg1)

	b1 := worker.NewBaseGroup(
		pl,
		"dispatch",
		1<<4,
		NewWorkerFunc,
		worker.DISPATCH_GROUP,
		nil,
	)
	pl.Insert(b1, "start")

	ew := worker.NewBaseGroup(
		pl,
		"end",
		1<<4,
		NewWorkerFunc,
		worker.NORMAL_GROUP,
		nil,
	)
	pl.Insert(ew, "dispatch")
	pl.Init()
	pl.Start()
	quit := make(chan bool)

	go func() {
		fmt.Println("Done Begin")
		pl.Done()
		quit <- true
		close(quit)
		fmt.Println("Done End")
	}()

	select {
	case <-quit:
		fmt.Println("Quit")
	}
}

func NewWorkerFunc(wg worker.Group, wIdx int) worker.Worker {
	switch wg.GetWorkerName() {
	case "start":
		//fmt.Println("NewWorkerFunc start InitFunc")
		return &A1Worker{
			BaseWorker: *worker.NewBaseWorker(wg, wIdx),
			Quit:       make(chan bool),
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
	Quit chan bool
}

func (aw *A1Worker) Init() error {
	fmt.Println("AW " + aw.WorkerGroup.GetWorkerName() + " worker[" + strconv.Itoa(aw.WorkerIdx) + "] Init")
	return nil

}

func (aw *A1Worker) Start() error {
	fmt.Println("AW " + aw.WorkerGroup.GetWorkerName() + " worker[" + strconv.Itoa(aw.WorkerIdx) + "] Start")
	aw.WorkerGroup.GetWaitGroup().Add(1)
	guidFactory := guid.NewGUIDFactory(int64(aw.WorkerIdx))
	go func() {
		for idx := 0; idx < 100; idx++ {
			curGuid, _ := guidFactory.NewGUID()
			fmt.Println("AW " + aw.WorkerGroup.GetWorkerName() + " worker[" + strconv.Itoa(aw.WorkerIdx) + "] Generate Msg {ID:" + strconv.FormatInt(int64(curGuid), 10) + ",Idx:" + strconv.Itoa(idx) + "}")
			aw.WorkerGroup.PutNextPipe(&Msg{ID: curGuid, Idx: idx})
			select {
			case <-aw.Quit:
				break
			default:
			}

		}
		fmt.Println("AW " + aw.WorkerGroup.GetWorkerName() + " worker[" + strconv.Itoa(aw.WorkerIdx) + "] End")
		aw.WorkerGroup.GetWaitGroup().Done()

	}()

	return nil
}

func (aw *A1Worker) Stop() error {
	aw.Quit <- true
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
	fmt.Println("BW " + bw.WorkerGroup.GetWorkerName() + " start_worker[" + strconv.Itoa(bw.WorkerIdx) + "] Start")
	bw.WorkerGroup.GetWaitGroup().Add(1)
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
		fmt.Println("BW " + bw.WorkerGroup.GetWorkerName() + " start_worker[" + strconv.Itoa(bw.WorkerIdx) + "] End")
		bw.WorkerGroup.GetWaitGroup().Done()
	}()
	return nil
}

func (bw *B1Worker) Dispatch() error {
	fmt.Println("BW " + bw.WorkerGroup.GetWorkerName() + " dispatch_worker[" + strconv.Itoa(bw.WorkerIdx) + "] StartDispatch")
	bw.WorkerGroup.GetDispatchWaitGroup().Add(1)
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
		fmt.Println("BW " + bw.WorkerGroup.GetWorkerName() + " dispatch_worker[" + strconv.Itoa(bw.WorkerIdx) + "] EndDispatch")
		bw.WorkerGroup.GetDispatchWaitGroup().Done()

	}()
	return nil
}

type EndWorker struct {
	worker.BaseWorker
}

func (ew *EndWorker) Init() error {
	fmt.Println("EW " + ew.WorkerGroup.GetWorkerName() + " worker[" + strconv.Itoa(ew.WorkerIdx) + "] Init")
	return nil
}

func (ew *EndWorker) Start() error {
	fmt.Println("EW " + ew.WorkerGroup.GetWorkerName() + " worker[" + strconv.Itoa(ew.WorkerIdx) + "] Start")
	ew.WorkerGroup.GetWaitGroup().Add(1)
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
		fmt.Println("EW " + ew.WorkerGroup.GetWorkerName() + " worker[" + strconv.Itoa(ew.WorkerIdx) + "] End")
		ew.WorkerGroup.GetWaitGroup().Done()
	}()
	return nil
}

func (ew *EndWorker) process(msgIn interface{}) (interface{}, error) {
	msg := msgIn.(*Msg)
	fmt.Println("Msg{ID:" + strconv.FormatInt(int64(msg.ID), 10) + ",Idx:" + strconv.Itoa(msg.Idx) + " Is Processing By EndWorker [" + strconv.Itoa(ew.WorkerIdx) + "]")
	return nil, nil
}
