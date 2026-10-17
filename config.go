package kafkaha

import (
	"crypto/tls"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	Brokers  []string
	Topic    string
	SaslType string
	SaslUser string
	SaslPass string
	Insecure bool
}

func (c Config) Opts() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
	}
	if c.SaslType != "" {
		opts = append(opts,
			kgo.SASL(newSASLMechanism(c.SaslType, c.SaslUser, c.SaslPass)),
		)
	}
	if c.Insecure {
		tlsConfig := tls.Config{
			InsecureSkipVerify: true,
		}
		opts = append(opts,
			kgo.DialTLSConfig(&tlsConfig),
		)
	}
	return opts
}
