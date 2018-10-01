package worker

import "fmt"

type Pipeline interface {
	Init() error
	Start() error
	Stop() error
	Insert(Group, ...string)
}

type BasePipeline struct {
	pipelines []Group
	wgNum     int
	wgMap     map[string]Group
}

func NewBasePipeline() *BasePipeline {
	return &BasePipeline{
		wgNum: 0,
		wgMap: make(map[string]Group),
	}
}

func (bp *BasePipeline) Init() error {
	fmt.Println("BasePipeline Init")
	for _, wg := range bp.pipelines {
		wg.Init()
	}
	return nil
}

func (bp *BasePipeline) Start() error {
	for idx := bp.wgNum - 1; idx >= 0; idx-- {
		bp.pipelines[idx].Start()
	}

	return nil
}

func (bp *BasePipeline) Stop() error {
	for _, wg := range bp.pipelines {
		wg.Stop()
	}
	return nil
}

func (bp *BasePipeline) Insert(wg Group, preWGNames ...string) {
	fmt.Println("Insert " + wg.GetWorkerName() + " to pipe")
	bp.pipelines = append(bp.pipelines, wg)
	if _, ok := bp.wgMap[wg.GetWorkerName()]; ok {
		panic("WorkerGroup Type Error, Some WorkerGroup" + wg.GetWorkerName())
	}
	bp.wgMap[wg.GetWorkerName()] = wg
	bp.wgNum = bp.wgNum + 1
	for _, preWGName := range preWGNames {
		if preWG, ok := bp.wgMap[preWGName]; ok {
			preWG.AddFollowQueue(wg.GetInQueue())
		} else {
			panic("BasePipeline Has No WorkerGroup Named" + preWGName)
		}
	}

}
