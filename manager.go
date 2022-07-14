package usago

import (
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)


type ChannelManager struct {
  connectionPool map[int]*amqp.Connection
  connectionMtxs map[int]sync.Mutex
}

func (mngr *ChannelManager) BuildChannel (bldr *ChannelBuilder) {
  
}
