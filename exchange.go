package amqp

import "github.com/streadway/amqp"

type (
	Exchange struct {
		Name string
		Kind string

		Durable bool
		AutoDelete bool
		Internal bool
		NoWait bool
		Args amqp.Table
		Passive bool

		Bindings []ExchangeBinding
	}

	ExchangeBinding struct {
		Source string
		RoutingKey string
		Destination string

		NoWait bool
		Args amqp.Table
	}
)
