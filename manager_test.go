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
	chnl, err := manager.NewChannel(*bldr)
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
	chnl, err := manager.NewChannel(*bldr)
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
	chnl, err := manager.NewChannel(*bldr)
	if err != nil {
		lgr.Error(
			"failed to create channel",
			zap.Error(err),
		)
		t.FailNow()
	}
	messageCount := 2
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
