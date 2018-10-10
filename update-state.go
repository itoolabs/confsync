package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/clientv3"
	"path/filepath"
)

var basePrefix string

func newUpdateStateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update-state [options] [-- <op> <key> <value>]+",
		Short: "updates service state in etcd",
		Long: `update-state command updates a set of keys in a single transaction and 
might be used to expose current state of certain service over etcd store.
update-state supports two operations:

 set key val

   sets <key> to <va> 

 del-if-same key val

   deletes <key> but only if it's current value set to <val>

Example:

confsync update-state --prefix /etc/router/state set backup MASTER -- set current backup
confsync update-state --prefix /etc/router/state set backup BACKUP -- del-if-same current backup
`,
		RunE: updateStateFunc,
		Args: cobra.MinimumNArgs(3),
	}
	cmd.Flags().StringVar(&basePrefix, "prefix", "", "`key` prefix for all the operations")
	return cmd
}

func updateStateFunc(cmd *cobra.Command, args []string) error {
	var ops []clientv3.Op
	for len(args) > 0 {
		if args[0] == "--" {
			args = args[1:]
		}
		switch len(args) {
		case 0:
			return errors.New("no command (trailing --?)")
		case 1:
			return errors.New("key missing")
		case 2:
			return errors.New("value missing")
		}
		cmd, key, value := args[0], args[1], args[2]
		if basePrefix != "" {
			key = filepath.Join(basePrefix, key)
		}
		switch cmd {
		case "set":
			ops = append(ops, clientv3.OpPut(key, value))
		case "del-if-same":
			ops = append(ops, clientv3.OpTxn(
				[]clientv3.Cmp{clientv3.Compare(clientv3.Value(key), "=", value)},
				[]clientv3.Op{clientv3.OpDelete(key)},
				[]clientv3.Op{},
			))
		default:
			return fmt.Errorf("unknown command %s", args[0])
		}
		args = args[3:]
	}
	c := mustClient()
	_, err := c.Txn(context.Background()).If().Then(ops...).Commit()
	if err != nil {
		return err
	}
	return nil
}
