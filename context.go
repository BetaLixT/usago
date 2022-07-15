package usago

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type requestChannel func(
	bldr ChannelBuilder,
) (*amqp.Channel, *chan amqp.Confirmation)

type ChannelContext struct {
	bldr         ChannelBuilder
	chnl         *amqp.Channel
	lgr          *zap.Logger
	reqChannel   requestChannel
	confirmsChan *chan amqp.Confirmation
	confirmsProx chan amqp.Confirmation
}

/*
Publishes a message and returns the sequence id of said message
Expect the fuction to block execution in case of a disconnection
event until a connection is re establilshed, the message resend
after re-connection must be handed user
Since the publish is tied to a channel, this function isn't to
be considered as thread safe
*/
func (ctx *ChannelContext) Publish() (uint64, error) {
	if ctx.chnl.IsClosed() {
		ctx.refreshChannel()
	}
	sqno := ctx.chnl.GetNextPublishSeqNo()
	if err := ctx.chnl.Publish(); err != nil {
		ctx.lgr.Warn("error while publishing event", zap.Error(err))
		// check if err can be fixed with re-connection
		ctx.refreshChannel()
		// caller must handle re-queuing of messages
		return 0, err
	}
	return sqno, nil
}

func (ctx *ChannelContext) refreshChannel() {
	ctx.lgr.Debug("refreshing channel...")
	ctx.chnl, ctx.confirmsChan = ctx.reqChannel(ctx.bldr)
}

func (ctx *ChannelContext) proxyConfirm(channel chan amqp.Confirmation) {
	active := true
	var cnfrm amqp.Confirmation
	for active {
		cnfrm, active = <- channel
		ctx.confirmsProx <- cnfrm
	}
}
