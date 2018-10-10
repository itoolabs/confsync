package main

import (
	"fmt"
	"github.com/coreos/etcd/client"
	"github.com/coreos/etcd/clientv3"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/grpc/grpclog"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

const (
	defaultDialTimeout      = 2 * time.Second
	defaultKeepAliveTime    = 2 * time.Second
	defaultKeepAliveTimeOut = 6 * time.Second
)

var (
	rootCmd = &cobra.Command{
		Use:        "confsync",
		Short:      "A simple tool to synchronize on-disk configuration files from etcd v3 cluster",
		SuggestFor: []string{"confsync"},
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if globals.debug {
				clientv3.SetLogger(grpclog.NewLoggerV2WithVerbosity(os.Stderr, os.Stderr, os.Stderr, 4))
			} else {
				clientv3.SetLogger(grpclog.NewLoggerV2(ioutil.Discard, ioutil.Discard, ioutil.Discard))
			}
		},
	}
	globals = struct {
		endpoints          []string
		dialTimeout        time.Duration
		keepAliveTime      time.Duration
		keepAliveTimeout   time.Duration
		insecure           bool
		insecureDiscovery  bool
		insecureSkipVerify bool
		debug bool
		tls                struct {
			certFile   string
			keyFile    string
			caFile     string
		}
		serviceName string
		user string
	}{}
)

func init() {
	rootCmd.PersistentFlags().StringSliceVarP(&globals.endpoints, "endpoints", "e", nil, "list of gRPC `endpoints`")
	rootCmd.PersistentFlags().StringVarP(&globals.serviceName, "domain", "d", "", "domain `name` to query for SRV records or to verify TLS-enabled secure servers against")
	rootCmd.PersistentFlags().StringVar(&globals.serviceName, "discovery-srv", "", "domain `name`, the same as --domain (to keep compatible with ETCDCTL_ variables)")


	rootCmd.PersistentFlags().BoolVar(&globals.debug, "debug", false, "enable client-side debug logging")

	rootCmd.PersistentFlags().DurationVar(&globals.dialTimeout, "dial-timeout", defaultDialTimeout, "dial `timeout` for client connections")
	rootCmd.PersistentFlags().DurationVar(&globals.keepAliveTime, "keepalive-time", defaultKeepAliveTime, "keepalive `time` for client connections")
	rootCmd.PersistentFlags().DurationVar(&globals.keepAliveTimeout, "keepalive-timeout", defaultKeepAliveTimeOut, "keepalive `timeout` for client connections")

	rootCmd.PersistentFlags().BoolVar(&globals.insecure, "insecure-transport", true, "disable transport security for client connections")
	rootCmd.PersistentFlags().BoolVar(&globals.insecureDiscovery, "insecure-discovery", true, "accept insecure SRV records describing cluster endpoints")
	rootCmd.PersistentFlags().BoolVar(&globals.insecureSkipVerify, "insecure-skip-tls-verify", false, "skip server certificate verification")

	rootCmd.PersistentFlags().StringVar(&globals.tls.certFile, "cert", "", "identify secure client using this TLS certificate `file`")
	rootCmd.PersistentFlags().StringVar(&globals.tls.keyFile, "key", "", "identify secure client using this TLS key `file`")
	rootCmd.PersistentFlags().StringVar(&globals.tls.caFile, "cacert", "", "verify certificates of TLS-enabled secure servers using this CA bundle `file`")

	rootCmd.PersistentFlags().StringVar(&globals.user, "user", "", "`username[:password]` for authentication (will prompt if password is not supplied)")

	rootCmd.AddCommand(
		newPutCommand(),
		newWatchCommand(),
		newUpdateStateCommand(),
	)

	cobra.EnablePrefixMatching = true
}

func fail(err error) {
	_, _ = fmt.Fprintln(os.Stderr, "Error:", err)
	if cerr, ok := err.(*client.ClusterError); ok {
		_, _ = fmt.Fprintln(os.Stderr, cerr.Detail())
	}
	os.Exit(1)
}

func main() {
	rootCmd.PersistentFlags().VisitAll(func (f *pflag.Flag) {
		envName := "ETCDCTL_" + strings.ToUpper(strings.Replace(f.Name, "-", "_", -1))
		if envValue := os.Getenv(envName); envValue != "" {
			if err := f.Value.Set(envValue); err != nil {
				fail(fmt.Errorf("error setting %s: %s", envName, err))
			}
		}
	})
	if err := rootCmd.Execute(); err != nil {
		fail(err)
	}
}
