package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/bgentry/speakeasy"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/srv"
	"go.etcd.io/etcd/pkg/transport"
	"os"
	"strings"
)

func mustClient() *clientv3.Client {
	var eps []string
	if len(globals.endpoints) > 0 {
		eps = globals.endpoints
	} else if globals.serviceName != "" {
		srvrs, err := srv.GetClient("etcd-client", globals.serviceName)
		if err != nil {
			fail(err)
		}
		eps = srvrs.Endpoints
		if !globals.insecureDiscovery {
			for i := 0; i < len(eps); {
				if strings.HasPrefix("http://", eps[i]) {
					fmt.Fprintf(os.Stderr, "ignoring discovered insecure endpoint %q\n", eps[i])
					copy(eps[i:], eps[i+1:])
					eps = eps[:len(eps)-1]
				} else {
					i++
				}
			}
		}
		if len(eps) == 0 {
			fail(fmt.Errorf("no endpoints discovered for %s", globals.serviceName))
		}
	} else {
		eps = []string{"127.0.0.1:2379"}
	}
	cfg := clientv3.Config{
		Endpoints:            eps,
		DialTimeout:          globals.dialTimeout,
		DialKeepAliveTime:    globals.keepAliveTime,
		DialKeepAliveTimeout: globals.keepAliveTimeout,
	}
	var (
		useTLS  bool
		tlsInfo transport.TLSInfo
	)
	if globals.tls.certFile == "" {
		if globals.tls.keyFile != "" {
			fail(errors.New("no TLS cert file specified"))
		}
	} else if globals.tls.keyFile == "" {
		fail(errors.New("no TLS key file specified"))
	} else {
		useTLS = true
		tlsInfo.CertFile = globals.tls.certFile
		tlsInfo.KeyFile = globals.tls.keyFile
	}
	if globals.tls.caFile != "" {
		useTLS = true
		tlsInfo.TrustedCAFile = globals.tls.caFile
	}
	if useTLS && globals.serviceName != "" {
		tlsInfo.ServerName = globals.serviceName
	}
	if useTLS {
		clientTLS, err := tlsInfo.ClientConfig()
		if err != nil {
			fail(err)
		}
		cfg.TLS = clientTLS
	}
	if cfg.TLS == nil && !globals.insecure {
		cfg.TLS = &tls.Config{}
	}
	if cfg.TLS != nil && globals.insecureSkipVerify {
		cfg.TLS.InsecureSkipVerify = true
	}

	if globals.user != "" {
		splitted := strings.SplitN(globals.user, ":", 2)
		if len(splitted) < 2 {
			cfg.Username = globals.user
			if pw, err := speakeasy.Ask("Password: "); err != nil {
				fail(err)
			} else {
				cfg.Password = pw
			}
		} else {
			cfg.Username = splitted[0]
			cfg.Password = splitted[1]
		}
	}
	client, err := clientv3.New(cfg)
	if err != nil {
		fail(err)
	}
	return client
}
