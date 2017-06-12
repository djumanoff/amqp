package amqp

import (
	"time"
	"github.com/streadway/amqp"
)

type (
	Client interface {
		Call(endpoint string, message Message) (*Message, error)
	}

	Publisher interface {
		Publish(message Message) error
	}

	ClientConfig struct {
		RequestX string
		ResponseX string
		ResponseQ string
	}

	client struct {
		sess *Session

		requestX string
		responseX string

		responseQ string

		rpcChannels map[string]chan Message

		close chan bool
	}
)

func (clt *client) run() error {
	queue, err := clt.sess.rec.QueueDeclare(
		clt.responseQ,
		false,
		true,
		false,
		false,
		nil,
	);
	if err != nil {
		clt.sess.log.Warn("QueueDeclare", err)
		return err
	}
	if err := clt.sess.rec.QueueBind(
		queue.Name,
		queue.Name,
		clt.responseX,
		false,
		nil,
	); err != nil {
		clt.sess.log.Warn("QueueBind", err)
		return err
	}

	msgs, err := clt.sess.rec.Consume(
		queue.Name,
		"",
		false,
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
	return clt.sess.sen.Publish(
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
	clt.rpcChannels[correlationId] = replyCh
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
	if ch, ok := clt.rpcChannels[chName]; ok {
		close(ch)
		delete(clt.rpcChannels, chName)
	}
}

func (clt *client) cleanup() error {
	clt.sess.log.Info("queue cleanup")

	if err := clt.sess.rec.QueueUnbind(
		clt.responseQ,
		clt.responseQ,
		clt.responseX,
		nil,
	); err != nil {
		clt.sess.log.Warn("QueueUnbind", err)
		return err
	}

	for k, ch := range clt.rpcChannels {
		close(ch)
		delete(clt.rpcChannels, k)
	}

	return nil
}
