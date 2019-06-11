package amqp

import (
	"time"

	"github.com/streadway/amqp"
	"fmt"
	"sync"
)

type (
	// Client interface which has Call method
	Client interface {
		Call(endpoint string, message Message) (*Message, error)
	}

	// Publisher interface which has Publish method
	Publisher interface {
		Publish(message Message) error
	}

	// ClientConfig struct
	ClientConfig struct {
		RequestX  string
		ResponseX string
		ResponseQ string
	}

	client struct {
		sess *session

		rec *amqp.Channel

		sen *amqp.Channel

		requestX  string
		responseX string

		responseQ string

		rpcChannelsMu sync.Mutex
		rpcChannels map[string]chan Message

		close chan bool
	}
)

func (clt *client) run() error {
	queue, err := clt.rec.QueueDeclare(
		clt.responseQ,
		false,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		clt.sess.log.Warn("QueueDeclare", err)
		return err
	}
	if err := clt.rec.QueueBind(
		queue.Name,
		queue.Name,
		clt.responseX,
		false,
		nil,
	); err != nil {
		clt.sess.log.Warn("QueueBind", err)
		return err
	}

	msgs, err := clt.rec.Consume(
		queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		clt.sess.log.Warn("Consume", err)
		return err
	}

	go func(msgs <-chan amqp.Delivery) {
		for {
			select {
			case d := <-msgs:
				if c, ok := clt.rpcChannels[d.CorrelationId]; ok {
					c <- deliveryToMessage(d)
				}
			case <-clt.close:
				fmt.Println("cleanup here?")
				clt.cleanup()
			}
		}
	}(msgs)

	return nil
}

func (clt *client) stop() {
	clt.close <- true
}

func (clt *client) Publish(message Message) error {
	message.Timestamp = time.Now()
	clt.sess.log.Debug(" -> ", message)
	return clt.sen.Publish(
		message.Exchange,
		message.RoutingKey,
		message.Mandatory,
		message.Immediate,
		message.publishing(),
	)
}

func (clt *client) Call(endpoint string, message Message) (*Message, error) {
	replyCh := make(chan Message)

	correlationId := correlationId(32)

	clt.rpcChannelsMu.Lock()
	clt.rpcChannels[correlationId] = replyCh
	clt.rpcChannelsMu.Unlock()

	defer clt.closeReplyChannel(correlationId)

	message.Exchange = clt.requestX
	message.RoutingKey = endpoint
	message.CorrelationId = correlationId
	message.ReplyTo = clt.responseQ
	message.MessageId = correlationId
	message.ContentType = "application/json"
	message.Mandatory = false
	message.Immediate = false

	if err := clt.Publish(message); err != nil {
		return nil, err
	}

	select {
	case reply := <-replyCh:
		clt.sess.log.Debug(" <- ", reply)
		return &reply, nil
	case <-time.After(10 * time.Second):
		return nil, ErrRpcTimeout
	}

	return nil, ErrUnknown
}

// Some helper methods
func (clt *client) closeReplyChannel(chName string) {
	clt.rpcChannelsMu.Lock()
	defer clt.rpcChannelsMu.Unlock()

	if ch, ok := clt.rpcChannels[chName]; ok {
		close(ch)
		delete(clt.rpcChannels, chName)
	}
}

func (clt *client) cleanup() error {
	clt.sess.log.Info("queue cleanup")

	if err := clt.rec.QueueUnbind(
		clt.responseQ,
		clt.responseQ,
		clt.responseX,
		nil,
	); err != nil {
		clt.sess.log.Warn("QueueUnbind", err)
		return err
	}

	clt.rpcChannelsMu.Lock()
	for k, ch := range clt.rpcChannels {
		close(ch)
		delete(clt.rpcChannels, k)
	}
	clt.rpcChannelsMu.Unlock()

	if err := clt.sen.Close(); err != nil {
		return err
	}
	return clt.rec.Close()
}
