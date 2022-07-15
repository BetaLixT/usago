package usago

import (
	"sync"
	"time"

	"github.com/BetaLixT/go-resiliency/retrier"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type ChannelManager struct {
	connectionPool []*amqp.Connection
	connectionMtxs []sync.Mutex
	nxtaccesscMtxs []sync.Mutex
	lowprirtycMtxs []sync.Mutex
	logger         *zap.Logger
	connRetry      retrier.Retrier
}

func (mngr *ChannelManager) RequestConnectionId() (int, error) {
	id := 0 // TODO: change to route somehow between multiple
	return id, nil
}

func (mngr *ChannelManager) GetConnection(id int) (*amqp.Connection, error) {
	// We need to mutex lock this but ensure that re connections get 
	// priority over the mutex
	mngr.lowprirtycMtxs[id].Lock()
	mngr.nxtaccesscMtxs[id].Lock()
	mngr.connectionMtxs[id].Lock()
	mngr.nxtaccesscMtxs[id].Unlock()
	defer mngr.connectionMtxs[id].Unlock()
	defer mngr.lowprirtycMtxs[id].Unlock()
	return mngr.connectionPool[id], nil
}

func (mngr *ChannelManager) NewChannel(
	bldr ChannelBuilder,
) (*channelContext, error) {
	id, err := mngr.RequestConnectionId()
	if err != nil {
		return nil, err
	}

	conn, err := mngr.GetConnection(id)
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

		var newChannel *amqp.Channel
		var newConfirm *chan amqp.Confirmation
		err := mngr.connRetry.Run(
			func() error {
				conn, err := mngr.GetConnection(id)
				if err != nil {
					return err
				}
				newChannel, newConfirm, err = bldr.Build(conn)
				// TODO: review errors
				if err != nil {
					return err
				}
				return nil
			},
		)
		if err != nil {
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
		pubRetr: *retrier.New(retrier.ExponentialBackoff(
			10,
			10*time.Millisecond,
		),
			retrier.DefaultClassifier{},
		),
	}
	return &ctx, nil
}
