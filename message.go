package amqp

import (
	"github.com/streadway/amqp"
	"time"
)

type (
	Table amqp.Table

	Message struct {
		Acknowledger amqp.Acknowledger `json:"-" bson:"-"` // the channel from which this delivery arrived

		Headers Table `json:"headers,omitempty" bson:"headers,omitempty"` // Application or header exchange table

					       // Properties
		ContentType     string `json:"contentType,omitempty" bson:"contentType,omitempty"`    // MIME content type
		ContentEncoding string `json:"contentEncoding,omitempty" bson:"contentEncoding,omitempty"`    // MIME content encoding
		DeliveryMode    uint8  `json:"deliveryMode,omitempty" bson:"deliveryMode,omitempty"`   // queue implemention use - non-persistent (1) or persistent (2)
		Priority        uint8  `json:"priority,omitempty" bson:"priority,omitempty"`   // queue implementation use - 0 to 9
		CorrelationId   string `json:"correlationId,omitempty" bson:"correlationId,omitempty"`   // application use - correlation identifier
		ReplyTo         string `json:"replyTo,omitempty" bson:"replyTo,omitempty"`    // application use - address to to reply to (ex: RPC)
		Expiration      string `json:"expiration,omitempty" bson:"expiration,omitempty"`    // implementation use - message expiration spec
		MessageId       string `json:"messageId,omitempty" bson:"messageId,omitempty"`    // application use - message identifier
		Timestamp       time.Time `json:"timestamp,omitempty" bson:"timestamp,omitempty"` // application use - message timestamp
		Type            string `json:"type,omitempty" bson:"type,omitempty"`    // application use - message type name
		UserId          string `json:"userId,omitempty" bson:"userId,omitempty"`    // application use - creating user - should be authenticated user
		AppId           string `json:"appId,omitempty" bson:"appId,omitempty"`    // application use - creating application id

					       // Valid only with Channel.Consume
		ConsumerTag string `json:"consumerTag,omitempty" bson:"consumerTag,omitempty"`

					       // Valid only with Channel.Get
		MessageCount uint32 `json:"messageCount,omitempty" bson:"messageCount,omitempty"`

		DeliveryTag uint64 `json:"deliveryTag,omitempty" bson:"deliveryTag,omitempty"`
		Redelivered bool `json:"redelivered,omitempty" bson:"redelivered,omitempty"`
		Exchange    string `json:"exchange,omitempty" bson:"exchange,omitempty"` // basic.publish exhange
		RoutingKey  string `json:"routingKey,omitempty" bson:"routingKey,omitempty"` // basic.publish routing key

		Body []byte `json:"-" bson:"-"`
		Mandatory bool `json:"mandatory,omitempty" bson:"mandatory,omitempty"`
		Immediate bool `json:"immediate,omitempty" bson:"immediate,omitempty"`

		Data interface{} `json:"data,omitempty" bson:"data,omitempty"`
	}

	Handler func(Message) *Message

	Middleware func(Handler) Handler

	Interceptor func(*Message) *Message
)

func (m Message) String() string {
	//m.BodyString = string(m.Body)
	//m.Body = nil
	//err := json.Unmarshal(m.Body, &m.Data)
	//if err != nil {
	//	return ""
	//}
	//
	//data, err := json.Marshal(m)
	//if err != nil {
	//	return ""
	//}

	return string(m.Body)
}

func (m Message) publishing() amqp.Publishing {
	return amqp.Publishing{
		Headers: amqp.Table(m.Headers),
		ContentType: m.ContentType,
		ContentEncoding: m.ContentEncoding,
		DeliveryMode: m.DeliveryMode,
		Priority: m.Priority,
		CorrelationId: m.CorrelationId,
		ReplyTo: m.ReplyTo,
		Expiration: m.Expiration,
		MessageId: m.MessageId,
		Timestamp: m.Timestamp,
		Type: m.Type,
		UserId: m.UserId,
		AppId: m.AppId,
		Body: m.Body,
	}
}

func (d Message) delivery() amqp.Delivery {
	return amqp.Delivery{
		Acknowledger: d.Acknowledger,
		Headers: amqp.Table(d.Headers),
		ContentType: d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode: d.DeliveryMode,
		Priority: d.Priority,
		CorrelationId: d.CorrelationId,
		ReplyTo: d.ReplyTo,
		Expiration: d.Expiration,
		MessageId: d.MessageId,
		Timestamp: d.Timestamp,
		Type: d.Type,
		UserId: d.UserId,
		AppId: d.AppId,
		ConsumerTag: d.ConsumerTag,
		MessageCount: d.MessageCount,
		DeliveryTag: d.DeliveryTag,
		Redelivered: d.Redelivered,
		Exchange: d.Exchange,
		RoutingKey: d.RoutingKey,
		Body: d.Body,
	}
}

func publishingToMessage(d amqp.Publishing) Message {
	return Message{
		Headers: Table(d.Headers),
		ContentType: d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode: d.DeliveryMode,
		Priority: d.Priority,
		CorrelationId: d.CorrelationId,
		ReplyTo: d.ReplyTo,
		Expiration: d.Expiration,
		MessageId: d.MessageId,
		Timestamp: d.Timestamp,
		Type: d.Type,
		UserId: d.UserId,
		AppId: d.AppId,
		Body: d.Body,
	}
}

func deliveryToMessage(d amqp.Delivery) Message {
	return Message{
		Acknowledger: d.Acknowledger,
		Headers: Table(d.Headers),
		ContentType: d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode: d.DeliveryMode,
		Priority: d.Priority,
		CorrelationId: d.CorrelationId,
		ReplyTo: d.ReplyTo,
		Expiration: d.Expiration,
		MessageId: d.MessageId,
		Timestamp: d.Timestamp,
		Type: d.Type,
		UserId: d.UserId,
		AppId: d.AppId,
		ConsumerTag: d.ConsumerTag,
		MessageCount: d.MessageCount,
		DeliveryTag: d.DeliveryTag,
		Redelivered: d.Redelivered,
		Exchange: d.Exchange,
		RoutingKey: d.RoutingKey,
		Body: d.Body,
	}
}
