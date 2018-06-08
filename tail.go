package gotail

import (
	"bufio"
	"io/ioutil"
	"os"
	"syscall"
	"time"

	yaml "gopkg.in/yaml.v2"
)

// Tail is tail file struct
type Tail struct {
	file                   string
	fileFd                 *os.File
	posFile                string
	posFd                  *os.File
	Stat                   Stat
	data                   chan []byte
	InitialReadPositionEnd bool // If true, there is no pos file Start reading from the end of the file
}

// Stat tail stats infomation struct
type Stat struct {
	Inode  uint64 `yaml:"Inode"`
	Offset int64  `yaml:"Offset"`
	Size   int64  `yaml:"Size"`
}

// Open file and position files.
func Open(file string, posfile string) (*Tail, error) {
	var err error
	var isCreatePosFile bool
	t := Tail{file: file, posFile: posfile}

	// open position file
	t.posFd, err = os.OpenFile(t.posFile, os.O_RDWR, 0644)
	if err != nil && !os.IsNotExist(err) {
		return &t, err
	} else if os.IsNotExist(err) {
		t.posFd, err = os.OpenFile(t.posFile, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return &t, err
		}
		isCreatePosFile = true
	}
	posdata, err := ioutil.ReadAll(t.posFd)
	if err != nil {
		return &t, err
	}
	posStat := Stat{}
	yaml.Unmarshal(posdata, &posStat)

	// open tail file.
	t.fileFd, err = os.Open(t.file)
	if err != nil {
		return &t, err
	}

	// get file stat
	fdStat, err := t.fileFd.Stat()
	if err != nil {
		return &t, err
	}
	stat := fdStat.Sys().(*syscall.Stat_t)

	// file stat
	t.Stat.Inode = stat.Ino
	t.Stat.Size = stat.Size
	if stat.Ino == posStat.Inode && stat.Size >= posStat.Size {
		// If the inode is not changed, restart from the subsequent Offset.
		t.Stat.Offset = posStat.Offset
	} else {
		// If the file size is small, set the offset to 0.
		t.Stat.Offset = 0
	}

	// update position file
	err = posUpdate(&t)
	if err != nil {
		return &t, err
	}

	// tail seek posititon.
	if isCreatePosFile {
		t.fileFd.Seek(0, os.SEEK_END)
	} else {
		t.fileFd.Seek(t.Stat.Offset, os.SEEK_SET)
	}

	return &t, nil
}

// Close is file and position file close.
func (t *Tail) Close() error {
	err := t.posFd.Close()
	if err != nil {
		return err
	}
	err = t.fileFd.Close()
	if err != nil {
		return err
	}

	return nil
}

func posUpdate(t *Tail) error {
	t.posFd.Truncate(0)
	t.posFd.Seek(0, 0)

	yml, err := yaml.Marshal(&t.Stat)
	if err != nil {
		return err
	}

	t.posFd.Write(yml)
	if err != nil {
		return err
	}

	t.posFd.Sync()
	return nil
}

// TailBytes is get one line bytes.
func (t *Tail) TailBytes() []byte {
	return <-t.data
}

// TailString is get one line strings.
func (t *Tail) TailString() string {
	return string(<-t.data)
}

// Scan is start scan.
func (t *Tail) Scan() {
	t.data = make(chan []byte)
	go func() {
		var err error
		for {
			scanner := bufio.NewScanner(t.fileFd)
			for scanner.Scan() {
				scbytes := scanner.Bytes()
				data := make([]byte, len(scbytes))
				copy(data, scanner.Bytes())
				t.data <- data
			}

			if err := scanner.Err(); err != nil {
				panic(err)
			}

			t.Stat.Offset, err = t.fileFd.Seek(0, os.SEEK_CUR)
			if err != nil {
				panic(err)
			}

			fd, err := os.Open(t.file)
			if os.IsNotExist(err) {
				time.Sleep(time.Millisecond * 10)
				continue
			} else if err != nil {
				panic(err)
			}
			fdStat, err := fd.Stat()
			if err != nil {
				panic(err)
			}
			stat := fdStat.Sys().(*syscall.Stat_t)
			if stat.Ino != t.Stat.Inode {
				t.Stat.Inode = stat.Ino
				t.Stat.Offset = 0
				t.Stat.Size = stat.Size
				t.fileFd.Close()
				t.fileFd = fd
			} else {
				if stat.Size < t.Stat.Size {
					t.fileFd.Seek(0, os.SEEK_SET)
				}
				t.Stat.Size = stat.Size
				time.Sleep(time.Millisecond * 10)
				fd.Close()
			}

			err = posUpdate(t)
			if err != nil {
				panic(err)
			}
		}
	}()
}
