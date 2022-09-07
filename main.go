package main

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"sarama-learning/config"
	"strings"

	"github.com/Shopify/sarama"
)

func main() {
	conf, err := config.Load()
	if err != nil {
		log.Fatalln(err)
		return
	}

	kafkaClient, err := newKafkaClient(conf)
	if err != nil {
		log.Fatalln(err)
	}

	clusterAdmin, err := sarama.NewClusterAdminFromClient(kafkaClient)
	if err != nil {
		log.Fatal("Error while creating cluster admin: ", err.Error())
	}
	defer func() { _ = clusterAdmin.Close() }()
	err = clusterAdmin.CreateTopic(conf.KafkaTopic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		log.Fatal("Error while creating topic: ", err.Error())
	}
}

func newKafkaClient(config config.Conf) (sarama.Client, error) {
	configKafka := sarama.NewConfig()
	configKafka.Version = sarama.V1_0_0_0
	configKafka.Consumer.Return.Errors = true

	if config.KafkaTLSEnabled {
		tlsConfig, err := newTLSConfig(config.KafkaTLSClientCert, config.KafkaTLSClientKey, config.KafkaTLSCACertFile)
		if err != nil {
			log.Fatal(nil, "setup kafka TLS error", err)
		}
		tlsConfig.InsecureSkipVerify = true

		configKafka.Net.TLS.Enable = true
		configKafka.Net.TLS.Config = tlsConfig
	}

	client, err := sarama.NewClient(strings.Split(config.KafkaBrokers, ","), configKafka)
	if err != nil {
		log.Fatal(nil, "error on initialing kafka connection", err)
	}

	return client, err
}

func newTLSConfig(clientCertFile, clientKeyFile, caCertFile string) (*tls.Config, error) {
	tlsConfig := tls.Config{}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		return &tlsConfig, err
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	// Load CA cert
	caCert, err := ioutil.ReadFile(caCertFile)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool
	return &tlsConfig, err
}
