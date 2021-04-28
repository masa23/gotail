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
	init                   bool
	done                   bool
	scanner                *bufio.Scanner
	err                    error
	isCreatePosFile        bool
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
	posStat := Stat{}
	t := Tail{
		file:    file,
		posFile: posfile,
		init:    true,
		done:    false,
	}

	// open position file
	if t.posFile != "" {
		t.posFd, err = os.OpenFile(t.posFile, os.O_RDWR, 0644)
		if err != nil && !os.IsNotExist(err) {
			return &t, err
		} else if os.IsNotExist(err) {
			t.posFd, err = os.OpenFile(t.posFile, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				return &t, err
			}
			t.isCreatePosFile = true
		}
		posdata, err := ioutil.ReadAll(t.posFd)
		if err != nil {
			return &t, err
		}
		err = yaml.Unmarshal(posdata, &posStat)
		if err != nil {
			return &t, err
		}
	}

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
	_, err = t.fileFd.Seek(t.Stat.Offset, os.SEEK_SET)
	if err != nil {
		return &t, err
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

// PositionUpdate is pos file update
func (t *Tail) PositionUpdate() {
	posUpdate(t)
}

func posUpdate(t *Tail) error {
	if t.posFile == "" {
		return nil
	}
	t.posFd.Truncate(0)
	t.posFd.Seek(0, 0)

	yml, err := yaml.Marshal(&t.Stat)
	if err != nil {
		return err
	}

	_, err = t.posFd.Write(yml)
	if err != nil {
		return err
	}

	err = t.posFd.Sync()
	if err != nil {
		return err
	}
	return nil
}

// Bytes is get one line bytes.
func (t *Tail) Bytes() []byte {
	return <-t.data
}

// Text is get one line strings.
func (t *Tail) Text() string {
	return string(<-t.data)
}

// Err is get Scan error
func (t *Tail) Err() error {
	return t.err
}

// Scan is start scan.
func (t *Tail) Scan() bool {
	var err error
	if t.done {
		return false
	}
	if t.init {
		// there is no pos file Start reading from the end of the file
		if (t.InitialReadPositionEnd && t.isCreatePosFile) ||
			(t.InitialReadPositionEnd && t.posFile == "") {
			t.fileFd.Seek(0, os.SEEK_END)
		}
		t.data = make(chan []byte, 1)
		t.scanner = bufio.NewScanner(t.fileFd)
		t.init = false
	}

	for {
		if t.scanner.Scan() {
			buf := t.scanner.Bytes()
			out := make([]byte, len(buf))
			copy(out, buf)
			t.data <- out
			return true
		}

		if err := t.scanner.Err(); err != nil {
			t.err = err
			return false
		}

		t.Stat.Offset, err = t.fileFd.Seek(0, os.SEEK_CUR)
		if err != nil {
			t.err = err
			return false
		}

		fd, err := os.Open(t.file)
		if os.IsNotExist(err) {
			time.Sleep(time.Millisecond * 10)
		} else if err != nil {
			t.err = err
			return false
		}
		fdStat, err := fd.Stat()
		if err != nil {
			t.err = err
			return false
		}
		stat := fdStat.Sys().(*syscall.Stat_t)
		if stat.Ino != t.Stat.Inode {
			t.Stat.Inode = stat.Ino
			t.Stat.Offset = 0
			t.Stat.Size = stat.Size
			_ = t.fileFd.Close()
			t.fileFd = fd
		} else {
			if stat.Size < t.Stat.Size {
				_, err = t.fileFd.Seek(0, os.SEEK_SET)
				if err != nil {
					t.err = err
					return false
				}
			}
			t.Stat.Size = stat.Size
			time.Sleep(time.Millisecond * 10)
			_ = fd.Close()
		}
		t.scanner = bufio.NewScanner(t.fileFd)

		err = posUpdate(t)
		if err != nil {
			t.err = err
			return false
		}
	}
}
