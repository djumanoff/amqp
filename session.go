// RabbitMQ driver with RPC functionality included, session object
// can be rpc server and client at the same time

package amqp

import (
	"os"
	"time"

	"github.com/streadway/amqp"
	//"os/signal"
	"errors"
	"math/rand"

	"github.com/sirupsen/logrus"
)

// Errors
var (
	ErrRpcTimeout          = errors.New("RPC timeout")
	ErrUnknown             = errors.New("Unknown error")
	ErrRPCChannelsRequired = errors.New("In order to make RPC calls you need to init receiver and sender")
)

const (
	defaultReqX = "request"
	defaultResX = "response"
)

type (
	Config struct {
		AMQPUrl     string
		Host        string
		VirtualHost string
		Port        int
		User        string
		Password    string
		LogLevel    uint8
	}

	Session struct {
		conn *amqp.Connection

		servers []*server
		clients []*client

		cfg Config

		close chan bool

		log *logrus.Logger
	}

	ConsumerConfig struct {
		PrefetchCount  int
		PrefetchSize   int
		PrefetchGlobal bool
	}

	PublisherConfig struct {
		// TODO: add publisher config options
	}
)

func NewSession(cfg Config) *Session {
	rand.Seed(time.Now().UTC().UnixNano())

	return &Session{
		cfg:   cfg,
		close: make(chan bool),

		servers: []*server{},
		clients: []*client{},

		log: &logrus.Logger{
			Out:   os.Stdout,
			Level: logrus.Level(cfg.LogLevel),
			Formatter: &logrus.TextFormatter{
				FullTimestamp: true,
			},
			//Formatter: &logrus.JSONFormatter{},
		},
	}
}

func (sess *Session) Consumer(consumerCfg ConsumerConfig) (Consumer, error) {
	rec, err := sess.conn.Channel()
	if err != nil {
		return nil, err
	}
	rec.Qos(consumerCfg.PrefetchCount, consumerCfg.PrefetchSize, consumerCfg.PrefetchGlobal)

	srv := &server{
		sess:  sess,
		qs:    []*Queue{},
		close: make(chan bool),
		rec:   rec,
	}

	go func() {
		sess.log.Fatal("rec channel is closing", <-rec.NotifyClose(make(chan *amqp.Error)))
		srv.Stop()
	}()

	sess.servers = append(sess.servers, srv)

	return srv, nil
}

func (sess *Session) Server(cfg ServerConfig) (Server, error) {
	if cfg.RequestX == "" {
		cfg.RequestX = defaultReqX
	}

	if cfg.ResponseX == "" {
		cfg.ResponseX = defaultResX
	}

	rec, err := sess.conn.Channel()
	if err != nil {
		return nil, err
	}

	sen, err := sess.conn.Channel()
	if err != nil {
		return nil, err
	}

	srv := &server{
		sess:      sess,
		responseX: cfg.ResponseX,
		requestX:  cfg.RequestX,
		qs:        []*Queue{},
		xs:        []*Exchange{},
		close:     make(chan bool),
		sen:       sen,
		rec:       rec,
	}

	go func() {
		sess.log.Fatal("sen channel is closing", <-sen.NotifyClose(make(chan *amqp.Error)))
		rec.Close()
		srv.Stop()
	}()

	go func() {
		sess.log.Fatal("rec channel is closing", <-rec.NotifyClose(make(chan *amqp.Error)))
		sen.Close()
		srv.Stop()
	}()

	sess.servers = append(sess.servers, srv)

	return srv, nil
}

func (sess *Session) Publisher(cfg PublisherConfig) (Publisher, error) {
	sen, err := sess.conn.Channel()
	if err != nil {
		return nil, err
	}

	go func() {
		sess.log.Fatal("sen channel is closing", <-sen.NotifyClose(make(chan *amqp.Error)))
	}()

	clt := &client{
		sess: sess,
		sen:  sen,
	}

	sess.clients = append(sess.clients, clt)

	return clt, nil
}

func (sess *Session) Client(cfg ClientConfig) (Client, error) {
	hostname := os.Getenv("HOSTNAME")
	if hostname == "" {
		hostname = "localhost"
	}
	correlationId := correlationId(32)

	if cfg.RequestX == "" {
		cfg.RequestX = defaultReqX
	}

	if cfg.ResponseX == "" {
		cfg.ResponseX = defaultResX
	}

	responseQ := cfg.ResponseQ
	if responseQ == "" {
		responseQ = cfg.ResponseX + "." + hostname + "." + correlationId
	}

	clt := &client{
		sess:        sess,
		responseX:   cfg.ResponseX,
		requestX:    cfg.RequestX,
		responseQ:   responseQ,
		rpcChannels: map[string]chan Message{},
		close:       make(chan bool),
	}

	sen, err := sess.conn.Channel()
	if err != nil {
		return nil, err
	}
	clt.sen = sen

	rec, err := sess.conn.Channel()
	if err != nil {
		return nil, err
	}
	clt.rec = rec

	go func() {
		sess.log.Fatal("sen channel is closing", <-sen.NotifyClose(make(chan *amqp.Error)))
		rec.Close()
		clt.stop()
	}()

	go func() {
		sess.log.Fatal("rec channel is closing", <-rec.NotifyClose(make(chan *amqp.Error)))
		sen.Close()
		clt.stop()
	}()

	clt.run()
	sess.clients = append(sess.clients, clt)

	return clt, nil
}

func (sess *Session) Connect() (err error) {
	if sess.cfg.AMQPUrl == "" {
		sess.cfg.AMQPUrl = buildUrl(sess.cfg)
	}

	sess.log.Info("connecting to ", sess.cfg.AMQPUrl)
	sess.conn, err = amqp.Dial(sess.cfg.AMQPUrl)
	if err != nil {
		sess.log.Warn("amqp.Dial", err)
		return err
	}

	go func() {
		sess.log.Fatal("connection is closing", <-sess.conn.NotifyClose(make(chan *amqp.Error)))
		sess.Close()
	}()

	return nil
}

func (sess *Session) Close() {
	sess.closeClients()
	sess.closeServers()

	sess.conn.Close()
}

func (sess *Session) closeServers() {
	for _, s := range sess.servers {
		s.Stop()
	}
}

func (sess *Session) closeClients() {
	for _, c := range sess.clients {
		c.stop()
	}
}
