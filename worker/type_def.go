package worker

type GROUP_TYPE int

const (
	_ GROUP_TYPE = iota
	DISPATCH_GROUP
	NORMAL_GROUP
)

type QUEUE_CAP_LEVEL int

const (
	_               QUEUE_CAP_LEVEL = iota
	QUEUE_CAP_ONE                   = 100
	QUEUE_CAP_TWO                   = 200
	QUEUE_CAP_THREE                 = 400
)
