package gotail

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"syscall"
	"time"

	"gopkg.in/yaml.v2"
)

var (
	// DefaultBufSize is default buffer size
	// If one line of log is too long, please adjust
	DefaultBufSize = 2008
	// If true, there is no pos file Start reading from the end of the file
	InitialReadPositionEnd = false
	// timeout for readLine
	ReadLineTimeout = 200 * time.Millisecond
)

// Tail is tail file struct
type Tail struct {
	file                   string
	fileFd                 *os.File
	posFile                string
	posFd                  *os.File
	Stat                   Stat
	buf                    []byte
	start                  int
	end                    int
	n                      int
	offset1                int64
	offset2                int64
	nextStart              int
	eofCount               int
	isEnd                  bool
	bufEmpty               bool
	init                   bool
	err                    error
	isCreatePosFile        bool
	InitialReadPositionEnd bool // deprecated
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
		file:     file,
		posFile:  posfile,
		init:     true,
		bufEmpty: true,
	}

	// compatibility maintenance
	if t.InitialReadPositionEnd {
		InitialReadPositionEnd = true
	}

	// create buffer
	t.buf = make([]byte, DefaultBufSize)

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
		t.offset1 = posStat.Offset
	} else {
		// If the file size is small, set the offset to 0.
		t.Stat.Offset = 0
	}

	// update position file
	err = t.PositionUpdate()
	if err != nil {
		return &t, err
	}

	// tail seek posititon.
	_, err = t.fileFd.Seek(t.Stat.Offset, io.SeekStart)
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
func (t *Tail) PositionUpdate() error {
	if t.posFile == "" {
		return nil
	}
	t.posFd.Truncate(0)
	t.posFd.Seek(0, io.SeekStart)

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
	return t.buf[t.start:t.end]
}

// Text is get one line strings.
func (t *Tail) Text() string {
	return string(t.Bytes())
}

// Err is get Scan error
func (t *Tail) Err() error {
	return t.err
}

// scanInit is only executed the first time Scan is run
func (t *Tail) scanInit() {
	if t.init {
		// there is no pos file Start reading from the end of the file
		if (InitialReadPositionEnd && t.isCreatePosFile) ||
			(InitialReadPositionEnd && t.posFile == "") {
			t.offset1, _ = t.fileFd.Seek(0, io.SeekEnd)
		}
		t.init = false
	}
}

// Scan is start scan.
func (t *Tail) Scan() bool {
	var err error
	// Executed only the first time
	t.scanInit()

	// Change start to new position
	t.start = t.nextStart

	for {
		// buffer empty
		if t.bufEmpty {
			// change offset
			t.offset2, _ = t.fileFd.Seek(t.offset1, io.SeekStart)

			// read file
			t.n, err = t.fileFd.Read(t.buf)
			if t.n == 0 || errors.Is(err, io.EOF) {
				// EOF file check
				t.eofCount++
				if t.eofCount > 5 {
					t.eofCount = 0
					t.fileCheck()
					continue
				}
				// sleep & next buffer read
				time.Sleep(ReadLineTimeout / 5)
				continue
			} else if err != nil {
				t.err = err
			}
			t.bufEmpty = false
		}
		t.eofCount = 0

		// search newline
		for i := t.start; i < t.n; i++ {
			if t.buf[i] == '\n' {
				t.end = i
				t.nextStart = i + 1
				t.isEnd = false
				return true
			}
		}

		// not found newline
		// Move offset to last newline
		t.offset1 = t.offset1 + int64(t.end)
		t.bufEmpty = true
		// If offset1 and offset2 are the same, the file has not been updated,
		// so wait a certain amount of time and read it again
		if t.offset1 == t.offset2 {
			if !t.isEnd {
				t.isEnd = true
				time.Sleep(ReadLineTimeout)
				continue
			}
			t.isEnd = false
			t.end = t.n
			// possiton update
			t.Stat.Offset = t.offset1 - 1
			t.PositionUpdate()
			return true
		} else {
			// Move offset by line feed code
			t.offset1++
			t.start = 0
			t.end = 0
			t.nextStart = 0
		}
	}
}

func (t *Tail) fileCheck() error {
	// status update
	fdstat, err := t.fileFd.Stat()
	if err != nil {
		return err
	}
	s := fdstat.Sys().(*syscall.Stat_t)
	t.Stat.Inode = s.Ino
	t.Stat.Size = s.Size
	t.Stat.Offset = t.offset1 - 1

	// update position file
	err = t.PositionUpdate()
	if err != nil {
		return err
	}

	// find new file
	for {
		// open file
		fd, err := os.Open(t.file)
		if os.IsNotExist(err) {
			// sleep & next file check
			time.Sleep(time.Second)
			continue
		} else if err != nil {
			return err
		}
		newFdStat, err := fd.Stat()
		if err != nil {
			return err
		}
		newStat := newFdStat.Sys().(*syscall.Stat_t)

		// If there is no change in inode and size, wait a little longer
		if t.Stat.Inode == newStat.Ino && t.Stat.Size == newStat.Size {
			fd.Close()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Replace any inode changes with new files
		if t.Stat.Inode != newStat.Ino {
			t.Stat.Inode = newStat.Ino
			t.Stat.Offset = 0
			t.offset1 = 0
			t.Stat.Size = newStat.Size
			t.fileFd.Close()
			t.fileFd = fd
			break
		}

		// If the size is smaller, move the SEEK position back to the beginning
		if newStat.Size < t.Stat.Size {
			_, err = t.fileFd.Seek(0, io.SeekStart)
			if err != nil {
				return err
			}
			t.Stat.Size = newStat.Size
			fd.Close()
			break
		}

		if newStat.Size > t.Stat.Size {
			_, err := t.fileFd.Seek(t.Stat.Offset, io.SeekStart)
			if err != nil {
				return err
			}
			fd.Close()
			break
		}
	}

	return nil
}
