package scas

import (
//  "fmt"
  "io"
  "io/ioutil"
  "os"
  "path"
  "strings"
  "log"
)

// SSTable is an immutable store of key/values.  The file structure is made up
// of three parts.  The size of the index, the index, and then the values.
// Size:
// size (4 bytes) # number of key / offset values
// Index: # array of pairs
// key    # 20 bytes
// offset # 4 bytes
// Values:
// size (4 bytes)
// value (variable number of bytes)
type ssTable struct {
  index map[Key] indexEntry
  dataFile *os.File
}

func newSSTable(d *os.File) (*ssTable, error) {
  if d == nil {
    var err error
    d, err = ioutil.TempFile(ssdir, SSDATA_PREFIX)
    if err != nil {
      return nil, err
    }
  }
  return &ssTable{index: make(map[Key]indexEntry), dataFile: d}, nil
}

type indexEntry struct {
  offset int
  size int
}

func (s ssTable) indexFile() (*os.File, error) {
  return os.Open(path.Join(ssdir, strings.Replace(s.dataFile.Name(), SSDATA_PREFIX, SSINDEX_PREFIX, 1)))
}

func (s ssTable) has(k *Key) (ok bool) {
  _, ok = s.index[*k]
  return
}

func (s ssTable) get(k *Key) (Value, error) {
  //log.Println("sstable get key", k)
  entry, ok := s.index[*k]
  if ok {
    buffer := make([]byte,entry.size)
    _, err := s.dataFile.ReadAt(buffer, int64(entry.offset))
    if err != nil {
      return nil, err
    }
    return Value(buffer), nil
  }
  return nil, nil
}

func (s ssTable) readIndex() error {
  f, err := s.indexFile()
  if err != nil {
    return err
  }
  for {
    var k Key
    n, err := f.Read(k[0:KeySize])
    if err == io.EOF && n==0 {
      return nil
    }
    if err != nil {
      return err
    }
    size, err := readInt(f)
    if err != nil {
      return err
    }
    offset, err := readInt(f)
    if err != nil {
      return err
    }
    s.index[k] = indexEntry{size:size, offset:offset}
    log.Println("loaded ssindex key / offset / size", &k, s.index[k])
  }
  return nil
}

func writeSSTable(mem memTable) (*ssTable, error) {
  ss, err := newSSTable(nil)
  if err != nil {
    return nil, err
  }
  indexFile, err := ss.indexFile()
  if err != nil {
    return nil, err
  }
  offset := 0
  for k, v := range mem.store {
    size := len(v)
    ss.index[k] = indexEntry{size: size}
    // TODO err handling
    indexFile.Write(k[0:KeySize])
    writeInt(indexFile,size)
    writeInt(indexFile,offset)
    offset += size
    ss.dataFile.Write(v)
  }
  return ss, nil
}

// read little endian int
func readInt(r io.Reader) (int, error) {
  buf := make([]byte,4)
  _, err := r.Read(buf)
  if err != nil {
    return 0, err
  }
  return int(buf[0] | buf[1]<<8 | buf[2]<<16 | buf[3]<<24), nil
}

// write little endian int
func writeInt(w io.Writer, n int) error {
  buf := make([]byte,4)
  buf[0] = byte(n & 0x000000ff)
  buf[1] = byte(n & 0x0000ff00 >>8)
  buf[2] = byte(n & 0x00ff0000 >>16)
  buf[3] = byte(n >>24)
  n,err := w.Write(buf)
  if err != nil {
    return err
  }
  return nil
}
