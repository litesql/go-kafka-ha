package kafkaha

import (
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func newSASLMechanism(typ, user, pass string) sasl.Mechanism {
	switch typ {
	case "plain":
		return plain.Auth{
			User: user,
			Pass: pass,
		}.AsMechanism()
	case "sha256":
		return scram.Auth{
			User: user,
			Pass: pass,
		}.AsSha256Mechanism()
	case "sha512":
		return scram.Auth{
			User: user,
			Pass: pass,
		}.AsSha512Mechanism()
	}
	return nil
}
