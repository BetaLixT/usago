package usago

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type exchangeOptions struct {
	name       string
	kind       string
	durable    bool
	autoDelete bool
	internal   bool
	noWait     bool
	args       map[string]interface{}
}

type queueOptions struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	noWait     bool
	args       map[string]interface{}
}

type serverNamedQueueOptions struct {
	internalName string
	durable      bool
	autoDelete   bool
	exclusive    bool
	noWait       bool
	args         map[string]interface{}
}

type bindingOptions struct {
	destKey string
	dest    string
	key     string
	source  string
	noWait  bool
	args    map[string]interface{}
}

type ChannelBuilder struct {
	exchanges  map[string]exchangeOptions
	queues     map[string]queueOptions
	servQueues map[string]serverNamedQueueOptions
	qbindings  []bindingOptions
	ebindings  []bindingOptions
	confirms   bool
	keyNames   map[string]string
}

func NewChannelBuilder() *ChannelBuilder {
	return &ChannelBuilder{
		exchanges:  map[string]exchangeOptions{},
		queues:     map[string]queueOptions{},
		servQueues: map[string]serverNamedQueueOptions{},
		qbindings:  []bindingOptions{},
		ebindings:  []bindingOptions{},
		keyNames:   map[string]string{},
	}
}

func (bldr *ChannelBuilder) GetKeydQueueName(key string) (string, error) {
	q, ok := bldr.keyNames[key]
	if !ok {
		return "", fmt.Errorf("queue not found")
	}
	return q, nil
}

func (bldr *ChannelBuilder) WithExchange(
	name,
	kind string,
	durable,
	autoDelete,
	internal,
	noWait bool,
	args map[string]interface{},
) *ChannelBuilder {
	bldr.exchanges[name] = exchangeOptions{
		name:       name,
		kind:       kind,
		durable:    durable,
		autoDelete: autoDelete,
		internal:   internal,
		noWait:     noWait,
		args:       args,
	}
	return bldr
}

func (bldr *ChannelBuilder) WithQueue(
	name string,
	durable,
	autoDelete,
	exclusive,
	noWait bool,
	args map[string]interface{},
) *ChannelBuilder {
	bldr.queues[name] = queueOptions{
		name:       name,
		durable:    durable,
		autoDelete: autoDelete,
		exclusive:  exclusive,
		noWait:     noWait,
		args:       args,
	}
	return bldr
}

func (bldr *ChannelBuilder) WithServerNamedQueue(
	queueKey string,
	durable,
	autoDelete,
	exclusive,
	noWait bool,
	args map[string]interface{},
) *ChannelBuilder {
	bldr.servQueues[queueKey] = serverNamedQueueOptions{
		internalName: queueKey,
		durable:      durable,
		autoDelete:   autoDelete,
		exclusive:    exclusive,
		noWait:       noWait,
		args:         args,
	}
	return bldr
}

func (bldr *ChannelBuilder) WithQueueBinding(
	name,
	key,
	exchange string,
	noWait bool,
	args map[string]interface{},
) *ChannelBuilder {
	bldr.qbindings = append(bldr.qbindings, bindingOptions{
		dest:   name,
		key:    key,
		source: exchange,
		noWait: noWait,
		args:   args,
	})
	return bldr
}

func (bldr *ChannelBuilder) WithServerNamedQueueBinding(
	queueKey,
	key,
	exchange string,
	noWait bool,
	args map[string]interface{},
) *ChannelBuilder {
	bldr.qbindings = append(bldr.qbindings, bindingOptions{
		destKey: queueKey,
		key:     key,
		source:  exchange,
		noWait:  noWait,
		args:    args,
	})
	return bldr
}

func (bldr *ChannelBuilder) WithExchangeBinding(
	dest,
	key,
	source string,
	noWait bool,
	args map[string]interface{},
) *ChannelBuilder {
	bldr.qbindings = append(bldr.qbindings, bindingOptions{
		dest:   dest,
		key:    key,
		source: source,
		noWait: noWait,
		args:   args,
	})
	return bldr
}

func (bldr *ChannelBuilder) WithConfirms(
	confirms bool,
) *ChannelBuilder {
	bldr.confirms = confirms
	return bldr
}

func (bldr *ChannelBuilder) Build(
	conn *amqp.Connection,
) (*amqp.Channel, *chan amqp.Confirmation, error) {
	ch, err := conn.Channel()
	// TODO: review what can go wrong here
	if err != nil {
		return nil, nil, err
	}
	for _, ex := range bldr.exchanges {
		err := ch.ExchangeDeclare(
			ex.name,
			ex.kind,
			ex.durable,
			ex.autoDelete,
			ex.internal,
			ex.noWait,
			ex.args,
		)
		// TODO: review what can go wrong here
		if err != nil {
			return nil, nil, err
		}
	}
	for _, q := range bldr.queues {
		_, err := ch.QueueDeclare(
			q.name,
			q.durable,
			q.autoDelete,
			q.exclusive,
			q.noWait,
			q.args,
		)
		// TODO: review what can go wrong here
		if err != nil {
			return nil, nil, err
		}
	}
	for _, q := range bldr.servQueues {
		n, err := ch.QueueDeclare(
			"",
			q.durable,
			q.autoDelete,
			q.exclusive,
			q.noWait,
			q.args,
		)
		if err != nil {
			return nil, nil, err
		}
		bldr.keyNames[q.internalName] = n.Name
	}
	for _, qb := range bldr.qbindings {
		dest := qb.dest
		if qb.destKey != "" {
			ok := false
			dest, ok = bldr.keyNames[qb.destKey]
			if !ok {
				return nil, nil, fmt.Errorf("queue key name not found")
			}
		}
		err := ch.QueueBind(
			dest,
			qb.key,
			qb.source,
			qb.noWait,
			qb.args,
		)
		// TODO: review what can go wrong here
		if err != nil {
			return nil, nil, err
		}
	}
	for _, eb := range bldr.ebindings {
		err := ch.ExchangeBind(
			eb.dest,
			eb.key,
			eb.source,
			eb.noWait,
			eb.args,
		)
		// TODO: review what can go wrong here
		if err != nil {
			return nil, nil, err
		}
	}
	if bldr.confirms {
		err := ch.Confirm(false)
		// TODO: review what can go wrong here
		if err != nil {
			return nil, nil, err
		}
		cnfrms := make(chan amqp.Confirmation, 1)
		ch.NotifyPublish(cnfrms)
		return ch, &cnfrms, nil
	}
	return ch, nil, nil
}
