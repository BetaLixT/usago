package usago

import (
	"sync"

	"github.com/BetaLixT/go-resiliency/retrier"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type requestChannel func(
	bldr ChannelBuilder,
) (*amqp.Channel, *chan amqp.Confirmation, error)

type channelContext struct {
	bldr         ChannelBuilder
	chnl         *amqp.Channel
	chnlMtx      sync.Mutex
	lgr          *zap.Logger
	reqChannel   requestChannel
	confirmsChan *chan amqp.Confirmation
	confirmsProx chan amqp.Confirmation
	closeChan    *chan *amqp.Error
	workerWg     sync.WaitGroup
	closing      bool
	borked       bool
	pubRetr      retrier.Retrier 
}

/*
Publishes a message and returns the sequence id of said message
Expect the fuction to block execution in case of a disconnection
event until a connection is re establilshed, the message resend
after re-connection must be handed user
Since the publish is tied to a channel, this function isn't to
be considered as thread safe
*/
func (ctx *channelContext) Publish(
	exchange string,
	key string,
	mandatory bool,
	immediate bool,
	msg amqp.Publishing,
) (uint64, error) {
	if ctx.closing {
		return 0, NewChannelClosedError()
	}
	if ctx.borked {
		return 0, NewChannelConnectionFailureError()
	}
	sqno := uint64(0)	
	err := ctx.pubRetr.Run(func() error {
		ctx.chnlMtx.Lock()
		defer ctx.chnlMtx.Unlock()
		sqno = ctx.chnl.GetNextPublishSeqNo()
		if err := ctx.chnl.Publish(
			exchange,
			key,
			mandatory,
			immediate,
			msg,
		); err != nil {
			ctx.lgr.Warn("error while publishing event", zap.Error(err))
			return err
		}
		return nil
	})	
	if err != nil {
		// caller must handle re-queuing of messages	
		return 0, err
	}
	return sqno, nil
}

func (ctx *channelContext) refreshChannel() error {
	ctx.chnlMtx.Lock()
	defer ctx.chnlMtx.Unlock()	
	ctx.chnl.Close() // apparently safe to call this multiple times, so no hurt	
	newchannel, newconfirms, err := ctx.reqChannel(ctx.bldr)
	if err != nil {
		return err
	}
	ctx.chnl = newchannel
	ctx.confirmsChan = newconfirms
	ctx.initNewChannel()
	return nil
}

func (ctx *channelContext) initNewChannel() {
	cls := make(chan *amqp.Error, 1)
	ctx.closeChan = &cls

	ctx.workerWg.Add(1)
	go func() {
		defer ctx.workerWg.Done()
		ctx.closeHandler(*ctx.closeChan)
	}()
	if ctx.confirmsChan != nil {
		ctx.workerWg.Add(1)
		go func() {
			defer ctx.workerWg.Done()
			ctx.proxyConfirm(*ctx.confirmsChan)
		}()	
	}
}

func (ctx *channelContext) close() {
	ctx.closing = true
	ctx.chnl.Close()
	ctx.workerWg.Wait()
}

func (ctx *channelContext) proxyConfirm(channel chan amqp.Confirmation) {
	active := true
	var cnfrm amqp.Confirmation
	for active {
		cnfrm, active = <-channel
		ctx.confirmsProx <- cnfrm
	}
}

func (ctx *channelContext) closeHandler(channel chan *amqp.Error) {
	_, _ = <- channel
	if ctx.closing {
		return
	}
	err := ctx.refreshChannel()
	if err != nil {
		ctx.lgr.Error(
			"channel has fatally failed",
			zap.Error(err),
		)
		ctx.borked = true
	}
}
