# Usago
Library that abstracts away and manages RabbitMQ connections and channels,
seamlessly handling temporary connection issues in the background and provides
an interface that is stable between reconnections for developers to use

The library is slightly opiniated since it uses uber's zap logger, this may or
may not work well with you and may be updated in the future to avoid that

## Installation
1. Install module
```
go get github.com/BetaLixT/usago
```
2. Import module 
```
import "github.com/BetaLixT/usago"
```

## Usage
Construct a new channel manager, providing it with a zap logger and a RabbitMQ
url (credentials included if any)
```go
logger, _ := zap.NewDevelopment()
manager := NewChannelManager(
  "amqp://guest:guest@localhost:5672/",
  logger,
)
```

Use the channel bulider to define your channel, providing all the exchanges,
queues and bindings required, the parameters for the builder functions is
essentially the same as what's in the respective
https://github.com/rabbitmq/amqp091-go package functions hence their
documentation can be useful if you're stuck
```go
bldr := NewChannelBuilder().WithQueue(
	"hello1",
	false,
	false,
	false,
	false,
	nil,
).WithConfirms(true)	
```

Once you have defined your channel, you can provide it to the channel manager
to have it build the channel for you and provide you with a channel context,
this function is blocking and will be blocked until the manger is able to 
establish a connection,
```go
chnl, err := manager.NewChannel(*bldr)
if err != nil {
	fmt.Printf("failed to create channel")
	return
}
```

You can now use the channel context to publish and consume messages, the consume
channel will be closed once the manager is closed, and publish will be blocking
when reconnections happen
```go
// consume
consumer, err := chnl.RegisterConsumer(
	"hello1",
	"",
	true,
	false,
	false,
	false,
	nil,
)
msg := <-consumer

// publish
body := "Hello World!"
sno, err = chnl.Publish(
	"",
	"hello1",
	false, // mandatory
	false, // immediate
	amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(body),
	},
)
```

**Do Not** close any channels provided by the library, these are to be handled
by usago, just call the Close() function on the manager once you're done using

The manager will enter a total failure state if it's unable make any succesful
connections in around 15 minutes

## TODO
* Implementation and handling of total failure state
* Better handling of closed state on manager
* Multiple connections handling (or moving to single only connection)

## Why did I build this?
Because I just thought of the name first and felt it's too perfect to not do
this..... Also I needed a rabbit mq library where I can abstract away conneciion
issues

## What's the name?
Usagi means rabbit in Japanese so it's a pun with rabbit + go
