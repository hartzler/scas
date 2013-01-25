package scas

import (
  "fmt"
  "os"
  "io"
  "io/ioutil"
  "log"
)

// the append only transaction log for writes
type txLog struct {
  file *os.File
}

func (t txLog) commit(k *Key, v Value) error {
  log.Println("logging tx key / value:", k, v)
  _, err := t.file.Write(k[0:20])
  if err != nil {
    return err
  }
  writeInt(t.file,len(v))
  _, err = t.file.Write(v)
  if err != nil {
    return err
  }
  err = t.file.Sync()
  if err != nil {
    return err
  }
  return nil
}

func (t txLog) replay(file *os.File, txHandler func(k *Key, v Value) error) error {
  var k Key
  for {
    n, err := file.Read(k[0:KeySize])
    if err != nil {
      if n==0 && err == io.EOF {
        return nil
      }
      return err
    }
    n, err = readInt(file)
    if err != nil {
      return err
    }
    v := make([]byte, n)
    n, err = file.Read(v)
    if err != nil {
      return err
    }
    err = txHandler(&k, v)
    if err != nil {
      return err
    }
  }
  return nil
}

func (t txLog) reset() error {
  t.file.Truncate(0)
  t.file.Seek(0,0)
  return nil
}

func newTxLog() txLog {
  f, err := ioutil.TempFile(txdir,LOGPREFIX)
  if err != nil {
    fmt.Println(err)
    panic("Can't get tempfile!")
  }
  return txLog{file: f}
}

