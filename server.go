package amqp

import (
	"time"

	"github.com/streadway/amqp"
)

type (
	Process interface {
		Start() error
		Stop() error
	}

	Server interface {
		Process
		Endpoint(endpoint string, handler Handler) error
	}

	Consumer interface {
		Process
		Queue(Queue) error
	}

	ServerConfig struct {
		RequestX  string
		ResponseX string
		UnbindQsAtStop bool
		UnbindExAtStop bool
	}

	server struct {
		sess *session

		rec *amqp.Channel

		sen *amqp.Channel

		requestX  string
		responseX string
		unbindQsAtStop bool
		unbindExAtStop bool

		qs []*Queue
		xs []*Exchange

		close chan bool
	}
)

func (srv *server) Stop() error {
	srv.close <- true
	return nil
}

func (srv *server) Start() error {
	if srv.requestX == defaultReqX {
		if err := srv.Exchange(Exchange{
			Name:       srv.requestX,
			Kind:       "direct",
			AutoDelete: false,
			Durable:    true,
			Internal:   false,
			NoWait:     true,
			Args:       nil,
		}); err != nil {
			srv.sess.log.Warn("InitExchange", err)
			return err
		}
	}

	if srv.responseX == defaultResX {
		if err := srv.Exchange(Exchange{
			Name:       srv.responseX,
			Kind:       "direct",
			AutoDelete: false,
			Durable:    true,
			Internal:   false,
			NoWait:     true,
			Args:       nil,
		}); err != nil {
			srv.sess.log.Warn("InitExchange", err)
			return err
		}
	}

	//if err := srv.initRpcExchanges(); err != nil {
	//	return err
	//}

	<-srv.close
	srv.cleanup()

	return nil
}

func (srv *server) InitRpcExchanges() error {
	if srv.requestX != "" {
		if err := srv.Exchange(Exchange{
			Name:       srv.requestX,
			Kind:       "direct",
			AutoDelete: false,
			Durable:    true,
			Internal:   false,
			NoWait:     true,
			Args:       nil,
		}); err != nil {
			srv.sess.log.Warn("InitExchange", err)
			return err
		}
	}

	if srv.responseX != "" {
		if err := srv.Exchange(Exchange{
			Name:       srv.responseX,
			Kind:       "direct",
			AutoDelete: false,
			Durable:    true,
			Internal:   false,
			NoWait:     true,
			Args:       nil,
		}); err != nil {
			srv.sess.log.Warn("InitExchange", err)
			return err
		}
	}

	return nil
}

func (srv *server) Exchange(x Exchange) error {
	if !x.Passive {
		if err := srv.sen.ExchangeDeclare(
			x.Name,
			x.Kind,
			x.Durable,
			x.AutoDelete,
			x.Internal,
			x.NoWait,
			x.Args,
		); err != nil {
			srv.sess.log.Warn("ExchangeDeclare", err, x)
			return err
		}
	} else {
		if err := srv.sen.ExchangeDeclarePassive(
			x.Name,
			x.Kind,
			x.Durable,
			x.AutoDelete,
			x.Internal,
			x.NoWait,
			x.Args,
		); err != nil {
			srv.sess.log.Warn("ExchangeDeclarePassive", err, x)
			return err
		}
	}

	for _, b := range x.Bindings {
		err := srv.sen.ExchangeBind(
			b.Destination,
			b.RoutingKey,
			b.Source,
			b.NoWait,
			b.Args,
		)
		if err != nil {
			srv.sess.log.Warn("ExchangeBind", err, b)
			return err
		}
	}

	srv.xs = append(srv.xs, &x)

	return nil
}

func (srv *server) initExchanges(xs []Exchange) error {
	for _, x := range xs {
		srv.Exchange(x)
	}

	return nil
}

func (srv *server) Queue(q Queue) error {
	if !q.Passive {
		queue, err := srv.rec.QueueDeclare(
			q.Name,
			q.Durable,
			q.AutoDelete,
			q.Exclusive,
			q.NoWait,
			q.Args,
		)
		if err != nil {
			srv.sess.log.Warn("QueueDeclare", err, q)
			return err
		}
		q.Name = queue.Name
		q.q = &queue
	} else {
		queue, err := srv.rec.QueueDeclarePassive(
			q.Name,
			q.Durable,
			q.AutoDelete,
			q.Exclusive,
			q.NoWait,
			q.Args,
		)
		if err != nil {
			srv.sess.log.Warn("QueueDeclarePassive", err, q)
			return err
		}
		q.Name = queue.Name
		q.q = &queue
	}

	for _, b := range q.Bindings {
		err := srv.rec.QueueBind(
			q.Name,
			b.RoutingKey,
			b.Exchange,
			b.NoWait,
			b.Args,
		)
		if err != nil {
			srv.sess.log.Warn("QueueBind", err, b)
			return err
		}

		msgs, err := srv.rec.Consume(
			q.Name,
			q.ConsumerTag,
			q.AutoAck,
			q.Exclusive,
			false,
			q.NoWait,
			q.Args,
		)
		if err != nil {
			srv.sess.log.Warn("QueueBind", err)
			return err
		}

		go func(ch <-chan amqp.Delivery, binding QueueBinding) {
			for del := range ch {
				go func(d amqp.Delivery) {
					m := deliveryToMessage(d)
					srv.sess.log.Debug(" <- ", m)

					msg := binding.Handler(m)
					if msg == nil {
						return
					}

					msg.ReplyTo = d.ReplyTo
					msg.CorrelationId = d.CorrelationId
					msg.AppId = d.AppId
					msg.Timestamp = time.Now()

					srv.sess.log.Debug(" -> ", msg)

					if err := srv.sen.Publish(
						srv.responseX,
						msg.ReplyTo,
						msg.Mandatory,
						msg.Immediate,
						msg.publishing(),
					); err != nil {
						srv.sess.log.Warn(err)
					}
				}(del)
			}
		}(msgs, b)
	}

	srv.qs = append(srv.qs, &q)

	return nil
}

func (srv *server) Endpoint(endpoint string, handler Handler) error {
	q := Queue{
		Name:       endpoint,
		Durable:    true,
		AutoDelete: true,
		Exclusive:  false,
		NoWait:     true,
		Args:       nil,
		AutoAck:    true,
		Bindings: []QueueBinding{
			{
				Name:       endpoint,
				Exchange:   srv.requestX,
				RoutingKey: endpoint,
				NoWait:     true,
				Args:       nil,
				Handler:    handler,
			},
		},
	}

	if err := srv.Queue(q); err != nil {
		srv.sess.log.Warn("Endpoint", err)
		return err
	}

	return nil
}

func (srv *server) cleanup() error {
	if srv.unbindQsAtStop {
		srv.sess.log.Info("started cleanup server rec channels")
		if err := srv.cleanupRec(); err != nil {
			return err
		}
	}

	if srv.unbindExAtStop {
		srv.sess.log.Info("started cleanup server sen channels")
		if err := srv.cleanupSen(); err != nil {
			return err
		}
	}

	return nil
}

func (srv *server) cleanupSen() error {
	srv.sess.log.Info("cleanupSen")

	for _, x := range srv.xs {
		for _, b := range x.Bindings {
			if err := srv.sen.ExchangeUnbind(
				b.Destination,
				b.RoutingKey,
				b.Source,
				b.NoWait,
				b.Args,
			); err != nil {
				srv.sess.log.Warn("ExchangeUnbind", err, b)
				return err
			}
		}
	}

	return srv.sen.Close()
}

func (srv *server) cleanupRec() error {
	srv.sess.log.Info("cleanupRec")

	for _, q := range srv.qs {
		for _, b := range q.Bindings {
			if err := srv.rec.QueueUnbind(
				q.Name,
				b.RoutingKey,
				b.Exchange,
				b.Args,
			); err != nil {
				srv.sess.log.Warn("QueueUnbind", err, b)
				return err
			}
		}
	}

	return srv.rec.Close()
}
