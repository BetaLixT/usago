package usago

type ChannelBuilder struct {
	exchanges map[string]struct {
		name       string
		kind       string
		autoDelete bool
		internal   bool
		noWait     bool
		args       map[string]interface{}
	}
	queues map[string]struct {
		name       string
		durable    bool
		autoDelete bool
		exclusive  bool
		noWait     bool
		args       map[string]interface{}
	}
	bindings map[string]struct {
		name     string
		key      string
		exchange string
		noWait   bool
		args     map[string]interface{}
	}
	confirms bool
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
	return bldr
}

func (bldr *ChannelBuilder) WithBinding(
	name,
	key,
	exchange string,
	noWait bool,
	args map[string]interface{},
) *ChannelBuilder {
	return bldr
}

func (bldr *ChannelBuilder) WithConfirms(
	confirms bool,
) *ChannelBuilder {
	bldr.confirms = confirms
	return bldr
}
