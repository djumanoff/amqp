package amqp

import (
	"time"

	"github.com/streadway/amqp"
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
		sess *Session

		rec *amqp.Channel

		sen *amqp.Channel

		requestX  string
		responseX string

		responseQ string

		rpcChannels rpcChannelsMx

		close chan bool
	}
)

type rpcChannelsMx struct {
	mx sync.RWMutex
	rpcChannelMap  map[string]chan Message
}
func (c *rpcChannelsMx) Store(key string, message chan Message) {
	c.mx.RLock()
	c.rpcChannelMap[key] = message
	c.mx.RUnlock()
}
func (c *rpcChannelsMx) Delete(key string)  {
	c.mx.RLock()
	delete(c.rpcChannelMap, key )
	c.mx.RUnlock()
}
func (c *rpcChannelsMx) Load(key string) (chan Message, bool) {
	message, ok := c.rpcChannelMap[key]
	return message, ok
}

func (c *rpcChannelsMx) RangeCleanUp() {
	c.mx.RLock()
	for key, ch := range  c.rpcChannelMap {
		close(ch)
		delete(c.rpcChannelMap, key )
	}
	c.mx.RUnlock()
}

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
				if c, ok := clt.rpcChannels.Load(d.CorrelationId); ok {
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
	clt.rpcChannels.Store(correlationId, replyCh)
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
	if ch, ok := clt.rpcChannels.Load(chName); ok {
		close(ch)
		clt.rpcChannels.Delete(chName)
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

	clt.rpcChannels.RangeCleanUp()

	return nil
}
