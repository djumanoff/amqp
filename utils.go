package amqp

import (
	"strconv"
	"math/rand"
)

const (
	amqpHost = "localhost"
	amqpPort = "5672"
)

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func correlationId(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func buildUrl(cfg Config) string {
	amqpUrl := "amqp://"

	if cfg.User != "" {
		amqpUrl += cfg.User
		if cfg.Password != "" {
			amqpUrl += ":" + cfg.Password
		}
		amqpUrl += "@"
	}

	if cfg.Host == "" {
		amqpUrl += amqpHost
	} else {
		amqpUrl += cfg.Host
	}

	port := strconv.Itoa(cfg.Port)
	if port == "" || port == "0" {
		port = amqpPort
	} else {
		amqpUrl += ":" + port
	}

	if cfg.VirtualHost != "" {
		amqpUrl += "/" + cfg.VirtualHost
	}

	return amqpUrl
}
