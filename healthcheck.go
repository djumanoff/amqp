package amqp

import "time"

func (sess *session) HealthCheck() error {
	clt, err := sess.createClient(ClientConfig{})
	if err != nil {
		return err
	}
	defer func() {
		clt.stop()
	}()

	srv, err := sess.createServer(ServerConfig{})
	if err != nil {
		return err
	}
	defer func() {
		srv.Stop()
	}()

	err = srv.Endpoint("health.check", func(msg Message) *Message {
		return &Message{Body: []byte("OK")}
	})
	if err != nil {
		return err
	}

	go func() {
		if err := srv.Start(); err != nil {
			sess.log.Warn(err.Error())
		}
	}()

	time.Sleep(100 * time.Millisecond)

	_, err = clt.Call("health.check", Message{})
	if err != nil {
		sess.log.Warn(err.Error())
		return err
	}

	return nil
}
