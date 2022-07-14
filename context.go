package usago

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ChannelContext struct {
  bldr ChannelBuilder
  chnl *amqp.Channel
}

func (ctx *ChannelContext) MustPublish() (uint64, error) {
  if ctx.chnl.IsClosed() {
    ctx.mustReconnect()
  }
  sqno := ctx.chnl.GetNextPublishSeqNo()
  if err := ctx.chnl.Publish(); err != nil {

    // check if err can be fixed with re-connection
    ctx.mustReconnect()

    // caller must handle re-queuing of messages
    return 0, err
  }
  return sqno, nil
}

func (ctx *ChannelContext) mustReconnect() {
  // Ensuring that the OnClose notification is being reached by
  // the connection first
  time.Sleep(1 * time.Millisecond)
  // get connection
  // rebuild channel
}
