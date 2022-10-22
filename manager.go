package usago

import (
	"fmt"
	"sync"
	"time"

	"github.com/BetaLixT/go-resiliency/retrier"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type ChannelManager struct {
	url           string
	connection    *amqp.Connection
	closeChannel  *chan *amqp.Error
	connectionMtx sync.Mutex
	nxtaccesscMtx sync.Mutex
	lowprirtycMtx sync.Mutex
	channelPool   map[int]*ChannelContext
	logger        *zap.Logger
	connRetry     retrier.Retrier
	closewg       sync.WaitGroup
	closing       bool
	borked        bool
	channelSq     int
	channelSqMtx  sync.Mutex
	channelClsMtx sync.Mutex
}

func NewChannelManager(
	url string,
	lgr *zap.Logger,
) *ChannelManager {
	connectionCount := 1
	mngr := &ChannelManager{
		url:         url,
		channelPool: map[int]*ChannelContext{},
		logger:      lgr,
		connRetry: *retrier.New(retrier.ExponentialBackoff(
			15,
			100*time.Millisecond,
		),
			retrier.DefaultClassifier{},
		),
		channelSq: -1,
	}
	for i := 0; i < connectionCount; i++ {
		err := mngr.establishConnection(i)
		if err != nil {
			panic(err)
		}
	}
	return mngr
}

func (mngr *ChannelManager) getConnection() (*amqp.Connection, error) {
	// We need to mutex lock this but ensure that re connections get
	// priority over the mutex
	mngr.logger.Info("attaining low priority connection lock")
	mngr.lowprirtycMtx.Lock()
	mngr.nxtaccesscMtx.Lock()
	mngr.connectionMtx.Lock()
	mngr.nxtaccesscMtx.Unlock()
	defer mngr.connectionMtx.Unlock()
	defer mngr.lowprirtycMtx.Unlock()
	mngr.logger.Info("low priority connection lock attained")
	mngr.logger.Info("returning connection")
	return mngr.connection, nil
}

func (mngr *ChannelManager) establishConnection(id int) error {
	mngr.logger.Info("attaining high priority connection lock")
	mngr.nxtaccesscMtx.Lock()
	mngr.connectionMtx.Lock()
	mngr.nxtaccesscMtx.Unlock()
	defer mngr.connectionMtx.Unlock()
	mngr.logger.Info("high priority connection lock attained")
	mngr.logger.Info("ensuring existing connection")

	// closing existing connection if it exists
	if mngr.connection != nil {
		// timeout on close because I have trust issues now
		clsdonechnl := make(chan error)
		go func() {
			defer close(clsdonechnl)
			mngr.connection.Close()
		}()
		select {
		case _, _ = <-clsdonechnl:
		case <-time.After(120 * time.Second):
			mngr.logger.Warn("timed out trying to connect to close connection")
		}
	}

	mngr.logger.Info("about to establish connection...")
	err := mngr.connRetry.Run(func() error {
		erchan := make(chan error)
		go func() {
			defer close(erchan)
			mngr.logger.Info("establishing connection...", zap.String("url", mngr.url))
			conn, err := amqp.Dial(mngr.url)
			if err != nil {
				mngr.logger.Warn(
					"connection dial failed",
					zap.Error(err),
				)
				erchan <- err
				return
			}
			mngr.connection = conn
			erchan <- nil
		}()
		select {
		case err := <-erchan:
			return err
		case <-time.After(120 * time.Second):
			mngr.logger.Error("timed out trying to connect to dial rabbitmq")
			return fmt.Errorf("timed out trying to connect to dial rabbitmq")
		}
	})
	if err != nil {
		mngr.logger.Error(
			"connection dial failed after multiple retries",
			zap.Error(err),
		)
		return err
	}

	mngr.logger.Info("re setup of close handlers...")
	closeChan := make(chan *amqp.Error)
	mngr.connection.NotifyClose(closeChan)
	mngr.closeChannel = &closeChan
	mngr.closewg.Add(1)
	go func() {
		defer mngr.closewg.Done()
		mngr.closeHandler(id, closeChan)
	}()
	mngr.logger.Info("connection re-established")
	return nil
}

func (mngr *ChannelManager) closeHandler(
	id int,
	clschan chan *amqp.Error,
) {
	err, _ := <-clschan
	if err != nil {
		mngr.logger.Warn(
			"connection has been unexpectedly closed",
			zap.Error(err),
		)
	} else {
		mngr.logger.Info("connection closed gracefully")
	}
	if !mngr.closing {
		err := mngr.establishConnection(id)
		if err != nil {
			mngr.borked = true
			mngr.Close()
		}
	} else {
		mngr.logger.Warn(
			"manager closing, connection will not be restablished",
			zap.Error(err),
		)
	}
}

func (mngr *ChannelManager) NewChannel(
	bldr ChannelBuilder,
) (*ChannelContext, error) {
	// TODO: no connection scenario
	if mngr.borked {
		return nil, NewChannelConnectionFailureError()
	}
	if mngr.closing {
		return nil, NewChannelClosedError()
	}

	conn, err := mngr.getConnection()
	if err != nil {
		return nil, err
	}
	chnl, cnfrm, err := bldr.Build(conn)
	// TODO: review errors
	if err != nil {
		return nil, err
	}

	reqChannel := func(bldr ChannelBuilder) (
		*amqp.Channel,
		*chan amqp.Confirmation,
		error,
	) {
		if mngr.borked {
			return nil, nil, NewChannelConnectionFailureError()
		}
		if mngr.closing {
			return nil, nil, NewChannelClosedError()
		}
		var newChannel *amqp.Channel
		var newConfirm *chan amqp.Confirmation
		err := mngr.connRetry.Run(
			func() error {
				mngr.logger.Info("refreshing channel...")
				conn, err := mngr.getConnection()
				if err != nil {
					mngr.logger.Warn(
						"failed to get connection while refreshing channel",
						zap.Error(err),
					)
					return err
				}
				mngr.logger.Info("retrieved connection")
				newChannel, newConfirm, err = bldr.Build(conn)
				// TODO: review errors
				if err != nil {
					mngr.logger.Warn(
						"failed to build channel while refreshing channel",
						zap.Error(err),
					)
					return err
				}
				mngr.logger.Info("built channel")
				return nil
			},
		)
		if err != nil {
			mngr.logger.Error(
				"channel creation failed after multiple attempts",
				zap.Error(err),
			)
			return nil, nil, err
		}
		return newChannel, newConfirm, nil
	}

	ctx := ChannelContext{
		id:           mngr.getNextChannelId(),
		bldr:         bldr,
		chnl:         chnl,
		lgr:          mngr.logger,
		reqChannel:   reqChannel,
		confirmsChan: cnfrm,
		confirmsProx: make(chan amqp.Confirmation, 10),
		closing:      false,
		borked:       false,
		pubRetr: *retrier.New(retrier.ExponentialBackoff(
			10,
			10*time.Millisecond,
		),
			retrier.DefaultClassifier{},
		),
		consumers: map[string]*consumerContext{},
	}
	ctx.initNewChannel()
	mngr.channelPool[ctx.id] = &ctx
	return &ctx, nil
}

func (mngr *ChannelManager) Close() {
	mngr.channelClsMtx.Lock()
	mngr.closing = true
	for _, ch := range mngr.channelPool {
		ch.close()
	}
	mngr.channelClsMtx.Unlock()
	mngr.connection.Close()
	mngr.closewg.Wait()
}

func (mngr *ChannelManager) Discard(ctxi *ChannelContext) error {
	mngr.channelClsMtx.Lock()
	defer mngr.channelClsMtx.Unlock()
	if ctx, exists := mngr.channelPool[ctxi.id]; exists {
		ctx.close()
		delete(mngr.channelPool, ctx.id)
		return nil
	}
	return NewChannelMissingError()
}

/*
Returns usago.Error with id of 1000 for closed or 1001 for failed manager
nil returned if there are no issues
*/
func (mngr *ChannelManager) Status() *Error {
	if mngr.borked {
		return NewChannelConnectionFailureError()
	}
	if mngr.closing {
		return NewChannelClosedError()
	}
	return nil
}

func (mngr *ChannelManager) getNextChannelId() int {
	mngr.channelSqMtx.Lock()
	defer mngr.channelSqMtx.Unlock()
	mngr.channelSq++
	return mngr.channelSq
}
