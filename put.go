package main

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"github.com/golang/snappy"
	"github.com/sabhiram/go-gitignore"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/clientv3"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strings"
)

func newPutCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "put [flags] <prefix> [<directory>]",
		Short: "Synchronizes the content of given directory to the store",
		Long: `Synchronizes the content of given directory to the etcd store under some prefix key namespace.

put command fill honor global .gitignore file and also .gitignore and .confignore files in given directory.
If directory is not absolute, put command will also consider .gitignore and .confignore in current 
working directory as well. 

put command will only update files if their content differ from those already stored. 

put command updates files in a single transaction. Since etcd limits both number of operations in a single
transaction and request limit, put command can handle about 40 files of totals size about 1 Mb (which
should be enough for most services).

If no directory given, put will synchronize content of current one.

Example:

confsync put /etc/firewall/keepalived

`,
		RunE: putCommandFunc,
		Args: cobra.RangeArgs(1, 2),
	}
	return cmd
}

type confIgnoreMatcher struct {
	gitIgnore  *ignore.GitIgnore
	confIgnore *ignore.GitIgnore
}

func (l *confIgnoreMatcher) MatchesPath(path string) bool {
	if l == nil {
		return false
	} else if l.gitIgnore != nil && l.gitIgnore.MatchesPath(path) {
		return true
	} else if l.confIgnore != nil && l.confIgnore.MatchesPath(path) {
		return true
	} else {
		return false
	}
}

type treeIgnoreMatcher struct {
	root   string
	global *ignore.GitIgnore
	local  map[string]confIgnoreMatcher
}

func newTreeIgnoreMatcher(root string) *treeIgnoreMatcher {
	var im = &treeIgnoreMatcher{
		root: root,
		local: make(map[string]confIgnoreMatcher),
	}
	if u, err := user.Current(); err == nil {
		if gi, err := ignore.CompileIgnoreFile(path.Join(u.HomeDir, ".gitignore_global")); err == nil {
			im.global = gi
		}
	}
	return im
}

func (tim *treeIgnoreMatcher) addPath(path string) {
	var (
		rel string
		err error
	)
	if rel, err = filepath.Rel(tim.root, path); err != nil || strings.HasPrefix(rel, "../") {
		return
	}
	cim := confIgnoreMatcher{}
	fp := filepath.Join(path, ".confignore")
	if fi, err := os.Stat(fp); err == nil && !fi.IsDir() {
		cim.confIgnore, err = ignore.CompileIgnoreFile(fp)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error compiling ignore file %s: %s\n", fp, err)
		}
	}
	fp = filepath.Join(path, ".gitignore")
	if fi, err := os.Stat(fp); err == nil && !fi.IsDir() {
		cim.gitIgnore, _ = ignore.CompileIgnoreFile(fp)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error compiling ignore file %s: %s\n", fp, err)
		}
	}
	if cim.confIgnore != nil || cim.gitIgnore != nil {
		tim.local[rel] = cim
	}
}

func (tim *treeIgnoreMatcher) Match(path string, isDir bool) bool {
	if tim.global != nil && tim.global.MatchesPath(path) {
		return true
	}
	rel, err := filepath.Rel(tim.root, path)
	if err != nil || strings.HasPrefix(rel, "../") {
		return false
	} else {
		path = rel
	}
	dir := path
	for {
		dir = filepath.Dir(dir)
		if cim, ok := tim.local[dir]; ok {
			if cim.MatchesPath(path) {
				return true
			}
		}
		if dir == "." || dir == "/" {
			break
		}
	}
	return false
}

func newHash() hash.Hash {
	return sha512.New512_224()
}

func putCommandFunc(cmd *cobra.Command, args []string) error {
	var root string
	if len(args) > 1 {
		root = args[1]
	}
	return updateTreeRecursively(mustClient(), args[0], root)
}

func getFile(path string) (data, hash []byte, err error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return
	}
	defer f.Close()
	h := newHash()
	data, err = ioutil.ReadAll(io.TeeReader(f, h))
	if err != nil {
		return
	}
	data = snappy.Encode(nil, data)
	s := h.Sum([]byte{})
	hash = make([]byte, hex.EncodedLen(len(s)))
	hex.Encode(hash, s)
	return
}

type opDesc struct {
	path  string
	isDel bool
}

func updateTreeRecursively(c clientv3.KV, prefix, root string) error {
	var (
		tree    = make(map[string]bool)
		ops     = make([]clientv3.Op, 0, 8)
		opsDesc = make([]opDesc, 0, 8)
		gi      *treeIgnoreMatcher
		cwd     string
		err     error
	)
	if cwd, err = os.Getwd(); err != nil {
		return fmt.Errorf("error getting current directory: %s", err)
	}
	resp, err := c.Get(context.Background(), path.Join(prefix, "/"), clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return err
	}
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if path.Base(key) != ".hash" {
			tree[string(kv.Key)] = true
		}
	}
	if root == "" {
		root = cwd
		gi = newTreeIgnoreMatcher(root)
	} else if filepath.IsAbs(root) {
		gi = newTreeIgnoreMatcher(root)
	} else {
		root = filepath.Join(cwd, root)
		if rel, err := filepath.Rel(cwd, root); err != nil {
			return fmt.Errorf("error getting source directory: %s", err)
		} else if strings.HasPrefix(rel, "../") {
			gi = newTreeIgnoreMatcher(root)
		} else {
			gi = newTreeIgnoreMatcher(cwd)
			gi.addPath(cwd)
		}
	}
	if err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if info.IsDir() {
			if info.Name() == ".git" {
				return filepath.SkipDir
			} else if gi.Match(p, true) {
				return filepath.SkipDir
			}
			gi.addPath(p)
			return nil
		}
		if info.Name() == ".gitignore" || info.Name() == ".confignore" || gi.Match(p, false) {
			return nil
		}
		data, digest, err := getFile(p)
		if err != nil {
			return fmt.Errorf("error reading file %s: %s", p, err)
		}
		rel, _ := filepath.Rel(root, p)
		key := filepath.Join(prefix, rel)
		hashKey := filepath.Join(key, ".hash")
		delete(tree, key)
		ops = append(ops, clientv3.OpTxn(
			[]clientv3.Cmp{
				clientv3.Compare(clientv3.CreateRevision(key), "!=", 0),
				clientv3.Compare(clientv3.CreateRevision(hashKey), "!=", 0),
				clientv3.Compare(clientv3.Value(hashKey), "=", string(digest)),
			},
			[]clientv3.Op{},
			[]clientv3.Op{
				clientv3.OpPut(key, string(data)),
				clientv3.OpPut(hashKey, string(digest)),
			},
		))
		opsDesc = append(opsDesc, opDesc{path: key})
		return nil
	}); err != nil {
		return err
	}
	for key := range tree {
		rel, _ := filepath.Rel(prefix, key)
		if !gi.Match(filepath.Join(root, rel), false) {
			ops = append(ops, clientv3.OpTxn(
				[]clientv3.Cmp{},
				[]clientv3.Op{
					clientv3.OpDelete(key),
					clientv3.OpDelete(path.Join(key, ".hash")),
				},
				[]clientv3.Op{},
			))
			opsDesc = append(opsDesc, opDesc{path: key, isDel: true})
		}
	}
	tresp, err := c.Txn(context.Background()).If().Then(ops...).Commit()
	if err != nil {
		return err
	}
	for i, r := range tresp.Responses {
		if r := r.GetResponseTxn(); r != nil {
			if opsDesc[i].isDel {
				if r.Succeeded {
					fmt.Printf("removed %s\n", opsDesc[i].path)
				}
			} else if !r.Succeeded {
				fmt.Printf("updated %s\n", opsDesc[i].path)
			}
		}
	}
	return nil
}
