package fluent

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/pkg/errors"
)

func dial(ctx context.Context, network, address string, timeout time.Duration, tlsConfig *tls.Config) (net.Conn, error) {
	connCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var dialer net.Dialer
	conn, err := dialer.DialContext(connCtx, network, address)
	if err != nil {
		return nil, errors.Wrap(err, `failed to connect to server`)
	}

	if tlsConfig != nil {
		client := tls.Client(conn, tlsConfig)
		if err = client.Handshake(); err != nil {
			return nil, errors.Wrap(err, `failed to handshale TLS`)
		}
		return client, nil
	}

	return conn, nil
}
