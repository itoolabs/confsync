package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/golang/snappy"
	"github.com/mattn/go-shellwords"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/clientv3"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	watchPrefix string
	keepalivedFifo string
	keepalivedPrefix string
	keepalivedInstance string
)

func newWatchCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "watch [flags] [-- <prefix> <root> <command> [<arg> ...] ]+",
		Short: "watches for changes in the story, synchronise with local file system and runs a command",
		Long: `watch command sets up a number of watchers waiting for changes under the prefix key,
synchronise store content to the local directory, and runs a local command (presumably, to reload 
certain service)

watch command also performs initial synchronisation and runs command if any file has been updated.

watch command may also listens for keepalived (http://www.keepalived.org) events FIFO and updates
keepalived state in the etcd store 

Example:
confsync watch --prefix /etc/firewall --ka-fifo /run/ka --ka-instance master --ka-key state \
      -- keepalived /services/keepalived/config sv reload keepalived 

`,
		RunE: watchCommandFunc,
		Args: cobra.MinimumNArgs(3),
	}
	cmd.Flags().StringVar(&watchPrefix, "prefix", "", "common `key` prefix for all watches and keepalived status")
	cmd.Flags().StringVar(&keepalivedFifo, "ka-fifo", "", "`path` to keepalived events FIFO")
	cmd.Flags().StringVar(&keepalivedInstance, "ka-instance", "", "keepalived instance `name`")
	cmd.Flags().StringVar(&keepalivedPrefix, "ka-key", "", "`key` prefix to store keepalived status (joined with --prefix, if set)")
	return cmd
}

type watcher struct {
	prefix string
	root   string
	cmd    string
	args   []string
}

func (w *watcher) runCmd() {
	cmd := exec.Cmd{
		Path:   w.cmd,
		Args:   w.args,
		Dir:    w.root,
		Env:    os.Environ(),
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error running command %s: %s\n", w.cmd, err)
	}
}

func (w *watcher) initialSync(c *clientv3.Client) int {
	resp, err := c.Get(clientv3.WithRequireLeader(context.Background()), w.prefix, clientv3.WithPrefix())
	if err != nil {
		fmt.Fprintf(os.Stderr, "initial sync failed for prefix %s root %s: %s\n", w.prefix, w.root, err)
		return 0
	}
	cnt := 0
	for _, kv := range resp.Kvs {
		key := strings.TrimPrefix(string(kv.Key), w.prefix)
		if path.Base(key) == ".hash" {
			continue
		}
		fn := filepath.Join(w.root, key)
		if data, err := snappy.Decode(nil, kv.Value); err != nil {
			fmt.Fprintf(os.Stderr, "error decompressing file %s content, skipping: %s", fn, err)
		} else if updated, err := w.maybeUpdateFile(fn, data); err != nil {
			fmt.Fprintf(os.Stderr, "failed to synchronize file %s: %s\n", fn, err)
		} else if updated {
			cnt++
		}
	}
	return cnt
}

func (w *watcher) maybeUpdateFile(path string, content []byte) (bool, error) {
	var p = filepath.Dir(path)
	if fi, err := os.Stat(path); err == nil {
		if fi.IsDir() {
			return false, fmt.Errorf("error updating file: %s is a direcotry", path)
		} else if fileContent, err := ioutil.ReadFile(path); err == nil {
			if bytes.Compare(fileContent, content) == 0 {
				return false, nil
			}
		}
	} else {
		if fi, err := os.Stat(p); err == nil {
			if !fi.IsDir() {
				return false, fmt.Errorf("error updating %s: %s is not a directory", path, p)
			}
		} else if err = os.MkdirAll(p, 0750); err != nil {
			return false, fmt.Errorf("error updating %s: can't create directory %s: %s", path, p, err)
		}
	}
	if f, err := ioutil.TempFile(p, ".temp*"); err != nil {
		return false, fmt.Errorf("error updating %s: %s", path, err)
	} else if _, err = f.Write(content); err != nil {
		return false, fmt.Errorf("error updating %s: %s", path, err)
	} else if err = f.Close(); err != nil {
		return false, fmt.Errorf("error updating %s: %s", path, err)
	} else if err = os.Rename(f.Name(), path); err != nil {
		return false, fmt.Errorf("error updating %s: %s", path, err)
	}
	return true, nil
}

func maybeRemoveDir(path string) (bool, error) {
	df, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return false, fmt.Errorf("error reading dir %s: %s", path, err)
	}
	defer df.Close()
	if dn, err := df.Readdirnames(1); err != nil && err != io.EOF {
		return false, fmt.Errorf("error reading dir %s: %s", path, err)
	} else if len(dn) != 0 {
		return false, nil
	} else if err = syscall.Rmdir(path); err != nil {
		return false, fmt.Errorf("error removing dir %s: %s", path, err)
	} else {
		return true, nil
	}
}

func (w *watcher) run(c *clientv3.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	ch := clientv3.NewWatcher(c).Watch(clientv3.WithRequireLeader(context.Background()), w.prefix, clientv3.WithPrefix())
	if w.initialSync(c) > 0 {
		w.runCmd()
	}
	for resp := range ch {
		if resp.Canceled {
			fmt.Fprintf(os.Stderr, "watch was canceled (%v)\n", resp.Err())
		}
		cnt := 0
		for _, ev := range resp.Events {
			key := strings.TrimPrefix(string(ev.Kv.Key), w.prefix)
			if path.Base(key) == ".hash" {
				continue
			}
			fn := path.Join(w.root, key)
			if ev.Type == clientv3.EventTypeDelete {
				if err := syscall.Unlink(fn); err != nil {
					fmt.Fprintf(os.Stderr, "error removing file %s: %s\n", fn, err)
				} else {
					fmt.Fprintf(os.Stdout, "removed %s\n", fn)
					d := fn
					for {
						d = filepath.Dir(d)
						if d == w.root {
							break
						} else if removed, err := maybeRemoveDir(d); err != nil {
							fmt.Fprintln(os.Stderr, err.Error())
						} else if removed {
							fmt.Fprintf(os.Stdout, "removed %s/\n", d)
						}
					}
				}
			} else if ev.Type == clientv3.EventTypePut {
				if data, err := snappy.Decode(nil, ev.Kv.Value); err != nil {
					fmt.Fprintf(os.Stderr, "error decompressing file %s content, skipping: %s", fn, err)
				} else if updated, err := w.maybeUpdateFile(fn, data); err != nil {
					fmt.Fprintln(os.Stderr, err.Error())
				} else if updated {
					cnt++
				}
			}
		}
		if cnt > 0 {
			w.runCmd()
		}
	}
}

func watchCommandFunc(cmd *cobra.Command, args []string) error {
	var watchers []*watcher
	for len(args) > 0 {
	Outer:
		switch len(args) {
		case 0:
			return errors.New("empty watcher definition (trailing --?)")
		case 1:
			return errors.New("watcher root directory missing")
		case 2:
			return errors.New("watcher command missing")
		}
		cmd, err := exec.LookPath(args[2])
		if err != nil {
			return fmt.Errorf("error finding command %s: %s", args[2], err)
		}
		watcher := &watcher{
			prefix: filepath.Join("/", watchPrefix, args[0]),
			root:   args[1],
			cmd:    cmd,
		}
		watchers = append(watchers, watcher)
		for i := 3; i < len(args); i++ {
			if args[i] == "--" {
				watcher.args = args[2:i]
				args = args[i+1:]
				goto Outer
			}
		}
		watcher.args = args[2:]
		break
	}
	if keepalivedFifo != "" && keepalivedInstance == "" {
		return fmt.Errorf("--ka-instance name must be set for processing keepalived events")
	} else if keepalivedInstance != "" && keepalivedFifo == "" {
		return fmt.Errorf("--ka-fifo name must be set for processing keepalived instance %s events", keepalivedInstance)
	} else if keepalivedFifo != "" && keepalivedInstance != "" {
		if watchPrefix != "" {
			keepalivedPrefix = filepath.Join("/", watchPrefix, keepalivedPrefix)
		}
	}
	return runWatchers(watchers)
}

func updateKeepalivedStatus(c *clientv3.Client, kind, instance, state string) {
	var (
		ops []clientv3.Op
	)
	ops = append(ops, clientv3.OpPut(filepath.Join(keepalivedPrefix, keepalivedInstance, kind, instance), state))
	ckey := filepath.Join(keepalivedPrefix, "current", kind, instance)
	if state == "MASTER" {
		ops = append(ops, clientv3.OpPut(ckey, keepalivedInstance))
	} else {
		ops = append(ops, clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Value(ckey), "=", keepalivedInstance)},
			[]clientv3.Op{clientv3.OpDelete(ckey)},
			[]clientv3.Op{},
		))
	}
	_, err := c.Txn(context.Background()).If().Then(ops...).Commit()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error updating keepalived status: %s", err)
	}
}

func runKeepaliveStateUpdater(c *clientv3.Client, wg *sync.WaitGroup, stop chan struct{}) {
	var (
		events = make(chan string)
		parser = shellwords.NewParser()
	)
	go runKeepalivedEventsListener(events)
Outer:
	for {
		select {
		case <-stop:
			break Outer
		case line := <-events:
			if args, err := parser.Parse(line); err != nil {
				fmt.Fprintf(os.Stderr, "error parsing keepalived event string %s: %s\n", line, err)
			} else if len(args) < 3 {
				fmt.Fprintf(os.Stderr, "error parsing keepalived event string %s: not enought parameters\n", line)
			} else {
				updateKeepalivedStatus(c, args[0], args[1], args[2])
			}
		}
	}
	wg.Done()
}

func runKeepalivedEventsListener(events chan string) {
	var wt *time.Timer
	for {
		if wt != nil {
			<-wt.C
		}
		fd, err := os.OpenFile(keepalivedFifo, os.O_RDONLY, 0)
		if err != nil {
			if wt == nil {
				wt = time.NewTimer(1 * time.Second)
			} else {
				wt.Reset(1 * time.Second)
			}
		} else {
			rdr := bufio.NewScanner(fd)
			for rdr.Scan() {
				events <- rdr.Text()
			}
			fd.Close()
		}
	}
}

func runWatchers(w []*watcher) error {
	c := mustClient()
	wg := &sync.WaitGroup{}
	dc := make(chan struct{})
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	if keepalivedFifo != "" {
		wg.Add(1)
		go runKeepaliveStateUpdater(c, wg, dc)
	}
	for i := range w {
		wg.Add(1)
		go w[i].run(c, wg)
	}
Loop:
	for {
		sig := <-sc
		switch sig {
		case syscall.SIGHUP:
			break Loop
		case syscall.SIGINT:
			break Loop
		case syscall.SIGTERM:
			break Loop
		case syscall.SIGQUIT:
			break Loop
		}
	}
	signal.Stop(sc)
	close(dc)
	err := c.Close()
	wg.Wait()
	return err
}
