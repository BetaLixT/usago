package usago

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

type bindingOptions struct {
	name     string
	key      string
	exchange string
	noWait   bool
	args     map[string]interface{}
}

type ChannelBuilder struct {
	exchanges map[string]exchangeOptions
	queues    map[string]queueOptions
	bindings  map[string]bindingOptions
	confirms  bool
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

func (bldr *ChannelBuilder) WithBinding(
	name,
	key,
	exchange string,
	noWait bool,
	args map[string]interface{},
) *ChannelBuilder {
	bldr.bindings[name] = bindingOptions{
		name:     name,
		key:      key,
		exchange: exchange,
		noWait:   noWait,
		args:     args,
	}
	return bldr
}

func (bldr *ChannelBuilder) WithConfirms(
	confirms bool,
) *ChannelBuilder {
	bldr.confirms = confirms
	return bldr
}
