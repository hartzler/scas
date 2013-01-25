package scas

import (
  "fmt"
  "os"
  "bytes"
  "encoding/hex"
  "path"
  "strings"
  "log"
)

var mem = newMemTable()
var tx txLog
var sstables []ssTable
var ssdir = "."
var txdir = "."

const KeySize = 20
const LOGPREFIX = "scas-log"
const SSDATA_PREFIX = "scas-ssdata"
const SSINDEX_PREFIX = "scas-ssindex"
type Key [KeySize]byte
type Value []byte

func (k *Key) String() string {
  buf := bytes.NewBuffer([]byte{})
  for _, b := range k {
    buf.WriteByte(b)
  }
  return hex.EncodeToString(buf.Bytes())
}

func (v Value) String() string {
  buf := bytes.NewBuffer([]byte{})
  for _, b := range v {
    buf.WriteByte(b)
  }
  return buf.String()
}

// checks for the existence of a key in any of the tables (mem or ss)
func exists(k *Key) bool {
  if mem.has(k) {
    return true
  }
  for _, ss := range sstables {
    if ss.has(k) {
      return true
    }
  }
  return false
}

// checks the memTable for presence of key.  If present, do nothing,
// else store the key/value in the txLog and then in the memTable
func Put(k *Key, v Value) error {
  if !exists(k) {
    err := tx.commit(k,v)
    err = mem.put(k,v)
    if err != nil {
      return err
    }
    if len(mem.store) > mem.maxSize {
      ss, err := mem.flush()
      if err != nil {
        return err
      }
      if ss != nil {
        sstables = append(sstables, *ss)
        mem = newMemTable()
        tx.reset()
      }
    }
  }
  return nil
}

func Get(k *Key) (v Value, err error) {
  v = mem.get(k)
  if v != nil {
    return
  }
  for _, ss := range sstables {
    v, err = ss.get(k)
    if err != nil {
      return
    }
    if v != nil {
      return
    }
  }
  return
}

func Close() error {
  _, err := mem.flush()
  if err != nil {
    log.Println("error flushing memtable",err)
    return err
  }
  // TODO close sstables
  return tx.reset()
}

// TODO support diff ss/tx dirs
func Init(txd, ssd string) {
  txdir = txd
  log.Println("initializing scas txdir: ",txdir)
  f, err := os.Open(txdir)
  if err != nil {
    log.Println(err)
    panic("error initializing scas txdir")
  }
  init_tx(f)

  log.Println("initializing scas ssdir: ",ssdir)
  f, err = os.Open(ssdir)
  if err != nil {
    log.Println(err)
    panic("error initializing scas ssdir")
  }
  init_ss(f)
}

func init_tx(f *os.File) error {
  tx = newTxLog()
  infos, err := f.Readdir(1024)
  if err != nil {
    fmt.Println(err)
    panic("error initializing scas")
  }
  log.Println("checking for old tx logs to replay...")
  for _, info := range infos {
    name := info.Name()
    if strings.HasPrefix(name, LOGPREFIX) && name != tx.file.Name() {
      log.Println("replaying tx log: ",name)
      fullpath := path.Join(txdir, name)
      oldfile, err := os.Open(fullpath)
      err = tx.replay(oldfile, func(k *Key, v Value) error {
        log.Println("recovering key / value:", k, v)
        return Put(k,v)
      })
      oldfile.Close()
      err = os.Remove(fullpath)
      if err != nil {
        fmt.Println("error removing old tx file!", err)
        // continue on for now... will keep getting replayed at startup.
      }
      log.Println("tx log replay complete.")
    }
  }
  return nil
}

func init_ss(f *os.File) error {
  infos, err := f.Readdir(1024)
  if err != nil {
    fmt.Println(err)
    panic("error initializing scas")
  }
  log.Println("loading sstables...")
  for _, info := range infos {
    if strings.HasPrefix(info.Name(), SSDATA_PREFIX) {
      log.Println("loading sstable: ",info.Name())
      datafile, err := os.Open(path.Join(ssdir, info.Name()))
      if err != nil {
        return err
      }
      ss,err := newSSTable(datafile)
      if err != nil {
        return err
      }
      err = ss.readIndex()
      if err != nil {
        fmt.Println("error loading sstable!", err)
        // continue on for now... will keep getting replayed at startup.
      }
      sstables = append(sstables, *ss)
    }
  }
  log.Println("loading sstables complete.")
  return nil
}
