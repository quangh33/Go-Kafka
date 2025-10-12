package broker

import "time"

const (
	sessionTimeout    = 10 * time.Second
	heartbeatInterval = 2 * time.Second
	rebalanceTimeout  = 3 * time.Second
)

const (
	Stable              ConsumerGroupState = "Stable"
	RebalanceInProgress ConsumerGroupState = "RebalanceInProgress"
)
