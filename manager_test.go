package usago

import (
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

func TestNewChannelManager(t *testing.T) {
	lgr, _ := zap.NewDevelopment()
	mngr := NewChannelManager(
		"amqp://guest:guest@localhost:5672/",
		lgr,
	)
	mngr.Close()
}

func TestPublishSimple(t *testing.T) {
	lgr, _ := zap.NewDevelopment()
	manager := NewChannelManager(
		"amqp://guest:guest@localhost:5672/",
		lgr,
	)

	bldr := NewChannelBuilder().WithQueue(
		"hello",
		false,
		false,
		false,
		false,
		nil,
	)
	chnl, err := manager.NewChannel(
		*bldr,
		[]StateUpdate{
			func(b bool) {
				lgr.Info("state updated", zap.Bool("state", b))
			},
		},
	)
	if err != nil {
		lgr.Error(
			"failed to create channel",
			zap.Error(err),
		)
	}
	body := "Hello World!"
	_, err = chnl.Publish(
		"",
		"hello",
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	if err != nil {
		lgr.Error(
			"failed to create channel",
			zap.Error(err),
		)
		t.FailNow()
	}
	manager.Close()
}

func TestPublishAck(t *testing.T) {
	lgr, _ := zap.NewDevelopment()
	manager := NewChannelManager(
		"amqp://guest:guest@localhost:5672/",
		lgr,
	)

	bldr := NewChannelBuilder().WithQueue(
		"hello",
		false,
		false,
		false,
		false,
		nil,
	).WithConfirms(true)
	chnl, err := manager.NewChannel(
		*bldr,
		[]StateUpdate{
			func(b bool) {
				lgr.Info("state updated", zap.Bool("state", b))
			},
		},
	)
	if err != nil {
		lgr.Error(
			"failed to create channel",
			zap.Error(err),
		)
		t.FailNow()
	}
	messageCount := 10
	cnfrms, err := chnl.GetConfirmsChannel()
	if err != nil {
		lgr.Error(
			"failed to get confirms channel",
			zap.Error(err),
		)
		t.FailNow()
	}

	wg := sync.WaitGroup{}
	count := 0
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < messageCount; i++ {
			ack := <-cnfrms
			lgr.Info("confirm recieved")
			if ack.Ack {
				count++
			} else {
				lgr.Error("failed delivery")
			}
		}
	}()

	body := "Hello World!"
	for i := 0; i < messageCount; i++ {
		_, err = chnl.Publish(
			"",
			"hello",
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			},
		)
		if err != nil {
			lgr.Error(
				"failed to publish message",
				zap.Error(err),
			)
			t.FailNow()
		}
	}
	wg.Wait()
	if count != messageCount {
		lgr.Error(
			"message count miss match",
			zap.Int("count", count),
			zap.Int("messageCount", messageCount),
		)
	}
	manager.Close()
}

// I'm just using this to test locally while killing and restarting the
// rabbitmq server
func TestConsumer(t *testing.T) {
	lgr, _ := zap.NewDevelopment()
	manager := NewChannelManager(
		"amqp://guest:guest@localhost:5672/",
		lgr,
	)

	bldr := NewChannelBuilder().WithQueue(
		"hello1",
		false,
		false,
		false,
		false,
		nil,
	).WithConfirms(true)
	chnl, err := manager.NewChannel(
		*bldr,
		[]StateUpdate{
			func(b bool) {
				lgr.Info("state updated", zap.Bool("state", b))
			},
		},
	)
	if err != nil {
		lgr.Error(
			"failed to create channel",
			zap.Error(err),
		)
		t.FailNow()
	}
	messageCount := 10
	cnfrms, err := chnl.GetConfirmsChannel()
	if err != nil {
		lgr.Error(
			"failed to get confirms channel",
			zap.Error(err),
		)
		t.FailNow()
	}

	wg := sync.WaitGroup{}

	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < messageCount; i++ {
			ack := <-cnfrms
			lgr.Info("confirm recieved")
			if ack.Ack {
			} else {
				lgr.Error("failed delivery")
			}
		}
	}()
	count := 0
	consumer, err := chnl.RegisterConsumer(
		"hello1",
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		lgr.Error(
			"failed to register consumer",
			zap.Error(err),
		)
		t.FailNow()
	}
	go func() {
		defer wg.Done()

		for i := 0; i < messageCount; i++ {
			msg := <-consumer
			lgr.Info(
				"message read",
				zap.String("body", string(msg.Body)),
			)
			count++
		}
	}()

	body := "Hello World!"
	for i := 0; i < messageCount; i++ {
		lgr.Info("publishing...")
		_, err = chnl.Publish(
			"",
			"hello1",
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			},
		)
		for err != nil {
			_, err = chnl.Publish(
				"",
				"hello1",
				false, // mandatory
				false, // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(body),
				},
			)
		}
	}
	wg.Wait()
	if count != messageCount {
		lgr.Error(
			"message count miss match",
			zap.Int("count", count),
			zap.Int("messageCount", messageCount),
		)
	}
	manager.Close()
}

func TestDelayedPub(t *testing.T) {
	lgr, _ := zap.NewDevelopment()
	manager := NewChannelManager(
		"amqp://guest:guest@localhost:5672/",
		lgr,
	)

	bldr := NewChannelBuilder().WithQueue(
		"hello",
		false,
		false,
		false,
		false,
		nil,
	).WithConfirms(true)
	chnl, err := manager.NewChannel(
		*bldr,
		[]StateUpdate{
			func(b bool) {
				lgr.Info("state updated", zap.Bool("state", b))
			},
		},
	)
	if err != nil {
		lgr.Error(
			"failed to create channel",
			zap.Error(err),
		)
		t.FailNow()
	}
	messageCount := 10
	cnfrms, err := chnl.GetConfirmsChannel()
	if err != nil {
		lgr.Error(
			"failed to get confirms channel",
			zap.Error(err),
		)
		t.FailNow()
	}

	wg := sync.WaitGroup{}
	count := 0
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < messageCount; i++ {
			ack := <-cnfrms
			lgr.Info("confirm recieved")
			if ack.Ack {
				count++
			} else {
				lgr.Error("failed delivery")
			}
		}
	}()

	body := "Hello World!"
	for i := 0; i < messageCount; i++ {
		time.Sleep(5 * time.Second)
		lgr.Info("publishing...")
		_, err = chnl.Publish(
			"",
			"hello",
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			},
		)
		for err != nil {
			_, err = chnl.Publish(
				"",
				"hello",
				false, // mandatory
				false, // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(body),
				},
			)
		}
	}
	wg.Wait()
	if count != messageCount {
		lgr.Error(
			"message count miss match",
			zap.Int("count", count),
			zap.Int("messageCount", messageCount),
		)
	}
}
