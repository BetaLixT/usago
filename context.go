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
	consumers    map[string]*consumerContext
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

func (ctx *channelContext) GetConfirmsChannel() (chan amqp.Confirmation, error) {
	if ctx.confirmsChan != nil {
		return ctx.confirmsProx, nil
	}
	return ctx.confirmsProx, NewNoConfirmsError()
}

func (ctx *channelContext) RegisterConsumer(
	queue,
	consumer string,
	autoAck,
	exclusive,
	noLocal,
	noWait bool,
	args map[string]interface{},
) (chan amqp.Delivery, error) {
	val, exists := ctx.consumers[consumer]
	if exists {
		return val.msgChan, nil
	}
	cnsmr := consumerContext{
		queue:     queue,
		consumer:  consumer,
		autoAck:   autoAck,
		exclusive: exclusive,
		noLocal:   noLocal,
		noWait:    noWait,
		args:      args,
		msgChan:   make(chan amqp.Delivery, 10),
	}
	err := ctx.initializeConsumer(&cnsmr)
	if err != nil {
		return cnsmr.msgChan, err
	}
	ctx.consumers[consumer] = &cnsmr
	return cnsmr.msgChan, nil
}

func (ctx *channelContext) initializeConsumer(consCtx *consumerContext) error {
	var cons <-chan amqp.Delivery
	var err error
	err = ctx.pubRetr.Run(func() error {
		ctx.lgr.Debug(
			"initializing consumer",
			zap.String("consumer", consCtx.consumer),
			zap.String("queue", consCtx.queue),
		)
		ctx.chnlMtx.Lock()
		defer ctx.chnlMtx.Unlock()
		cons, err = ctx.chnl.Consume(
			consCtx.queue,
			consCtx.consumer,
			consCtx.autoAck,
			consCtx.exclusive,
			consCtx.noLocal,
			consCtx.noWait,
			consCtx.args,
		)
		if err != nil {
			ctx.lgr.Warn(
				"Failed to initialize consumer",
				zap.Error(err),
				zap.String("consumer", consCtx.consumer),
				zap.String("queue", consCtx.queue),
			)
		}
		return err
	})
	if err != nil {
		// caller must handle re-queuing of messages
		return err
	}
	ctx.workerWg.Add(1)
	go func() {
		defer ctx.workerWg.Done()
		for msg := range cons {
			consCtx.msgChan <- msg
		}
	}()
	return nil
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
	ctx.chnl.NotifyClose(cls)
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
	close(ctx.confirmsProx)
	for _, cnsmr := range ctx.consumers {
		close(cnsmr.msgChan)
	}
	ctx.workerWg.Wait()
}

func (ctx *channelContext) proxyConfirm(channel chan amqp.Confirmation) {
	active := true
	var cnfrm amqp.Confirmation
	for active {
		cnfrm, active = <-channel
		if active {
			ctx.confirmsProx <- cnfrm
		}
	}
}

func (ctx *channelContext) closeHandler(channel chan *amqp.Error) {
	_, _ = <-channel
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
	} else {
		for key, cnsmr := range ctx.consumers {
			err := ctx.initializeConsumer(cnsmr)
			if err != nil {
				ctx.lgr.Error(
					"Failed to initialize consumer",
					zap.Error(err),
					zap.String("consumer", cnsmr.consumer),
					zap.String("queue", cnsmr.queue),
				)
				close(cnsmr.msgChan)
				delete(ctx.consumers, key)
			}
		}
	}
}

type consumerContext struct {
	queue     string
	consumer  string
	autoAck   bool
	exclusive bool
	noLocal   bool
	noWait    bool
	args      map[string]interface{}
	msgChan   chan amqp.Delivery
}
