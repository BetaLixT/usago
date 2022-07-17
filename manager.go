package usago

import (
	"sync"
	"time"

	"github.com/BetaLixT/go-resiliency/retrier"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type ChannelManager struct {
	url            string
	connectionPool []*amqp.Connection
	closeChannels  []*chan *amqp.Error
	connectionMtxs []sync.Mutex
	nxtaccesscMtxs []sync.Mutex
	lowprirtycMtxs []sync.Mutex
	channelPool    []*channelContext
	logger         *zap.Logger
	connRetry      retrier.Retrier
	closewg        sync.WaitGroup
	closing        bool
	borked         bool
}

func NewChannelManager(
	url string,
	lgr *zap.Logger,
) *ChannelManager {
	connectionCount := 1
	mngr := &ChannelManager{
		url:            url,
		connectionPool: make([]*amqp.Connection, connectionCount),
		closeChannels:  make([]*chan *amqp.Error, connectionCount),
		connectionMtxs: make([]sync.Mutex, connectionCount),
		nxtaccesscMtxs: make([]sync.Mutex, connectionCount),
		lowprirtycMtxs: make([]sync.Mutex, connectionCount),
		channelPool:    []*channelContext{},
		logger:         lgr,
		connRetry: *retrier.New(retrier.ExponentialBackoff(
			15,
			100*time.Millisecond,
		),
			retrier.DefaultClassifier{},
		),
	}
	for i := 0; i < connectionCount; i++ {
		err := mngr.establishConnection(i)
		if err != nil {
			panic(err)
		}
	}
	return mngr
}

func (mngr *ChannelManager) requestConnectionId() (int, error) {
	id := 0 // TODO: change to route somehow between multiple
	return id, nil
}

func (mngr *ChannelManager) getConnection(id int) (*amqp.Connection, error) {
	// We need to mutex lock this but ensure that re connections get
	// priority over the mutex
	mngr.lowprirtycMtxs[id].Lock()
	mngr.nxtaccesscMtxs[id].Lock()
	mngr.connectionMtxs[id].Lock()
	mngr.nxtaccesscMtxs[id].Unlock()
	defer mngr.connectionMtxs[id].Unlock()
	defer mngr.lowprirtycMtxs[id].Unlock()
	if id > len(mngr.connectionPool) {
		return nil, NewConnectionMissingError()
	}
	conn := mngr.connectionPool[id]
	if conn == nil {
		return nil, NewConnectionMissingError()
	}
	return conn, nil
}

func (mngr *ChannelManager) establishConnection(id int) error {
	mngr.nxtaccesscMtxs[id].Lock()
	mngr.connectionMtxs[id].Lock()
	mngr.nxtaccesscMtxs[id].Unlock()
	defer mngr.connectionMtxs[id].Unlock()
	err := mngr.connRetry.Run(func() error {
		mngr.logger.Debug("establishing connection...")
		conn, err := amqp.Dial(mngr.url)
		if err != nil {
			mngr.logger.Warn(
				"connection dial failed",
				zap.Error(err),
			)
			return err
		}
		mngr.connectionPool[id] = conn
		return nil
	})
	if err != nil {
		mngr.logger.Error(
			"connection dial failed after multiple retries",
			zap.Error(err),
		)
		return err
	}
	closeChan := make(chan *amqp.Error)
  mngr.connectionPool[id].NotifyClose(closeChan)
	mngr.closeChannels[id] = &closeChan
	mngr.closewg.Add(1)
	go func() {
		defer mngr.closewg.Done()
		mngr.closeHandler(id, closeChan)
	}()
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
		mngr.logger.Debug("connection closed gracefully")
	}
	if !mngr.closing {
		err := mngr.establishConnection(id)
		if err != nil {
			mngr.borked = true
			mngr.Close()
		}
	}
}

func (mngr *ChannelManager) NewChannel(
	bldr ChannelBuilder,
) (*channelContext, error) {
	// TODO: no connection scenario
	if mngr.borked {
		return nil, NewChannelConnectionFailureError()
	}
	if mngr.closing {
		return nil, NewChannelClosedError()
	}
	id, err := mngr.requestConnectionId()
	if err != nil {
		return nil, err
	}

	conn, err := mngr.getConnection(id)
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
				mngr.logger.Debug("refreshing channel...")
				conn, err := mngr.getConnection(id)
				if err != nil {
					mngr.logger.Warn(
						"failed to get connection while refreshing channel",
						zap.Error(err),
					)
					return err
				}
				newChannel, newConfirm, err = bldr.Build(conn)
				// TODO: review errors
				if err != nil {
					mngr.logger.Warn(
						"failed to build channel while refreshing channel",
						zap.Error(err),
					)
					return err
				}
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

	ctx := channelContext{
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
	mngr.channelPool = append(mngr.channelPool, &ctx)
	return &ctx, nil
}

func (mngr *ChannelManager) Close() {
	mngr.closing = true
	for _, ch := range mngr.channelPool {
		ch.close()
	}
	for _, conn := range mngr.connectionPool {
		conn.Close()
	}
	mngr.closewg.Wait()
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
