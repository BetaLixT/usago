package usago

type ChannelBuilder struct {

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
