package config

import (
	"log"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

type Conf struct {
	KafkaBrokers       string `envconfig:"KAFKA_BROKERS" required:"true"`
	KafkaTopic         string `envconfig:"KAFKA_TOPICS" required:"true"`
	KafkaConsumerGroup string `envconfig:"KAFKA_CONSUMER_GROUP" required:"true"`

	KafkaTLSCACertFile string `envconfig:"KAFKA_TLS_CA_CERT_FILE" default:"/secrets/ca_cert.pem"`
	KafkaTLSClientCert string `envconfig:"KAFKA_TLS_CLIENT_CERT" default:"/secrets/client_cert.pem"`
	KafkaTLSClientKey  string `envconfig:"KAFKA_TLS_CLIENT_KEY" default:"/secrets/client_key.pem"`
	KafkaTLSEnabled    bool   `envconfig:"KAFKA_TLS_ENABLED" default:"false"`
}

func Load() (Conf, error) {
	godotenv.Load()
	c := Conf{}
	if err := envconfig.Process("", &c); err != nil {
		log.Fatal(nil, "load env error", err)
	}
	log.Println(c.KafkaTLSEnabled)
	return c, nil
}
