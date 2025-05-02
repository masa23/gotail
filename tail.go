package gotail

import (
	"errors"
	"io"
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
	start, end, n          int
	offset1, offset2       int64
	nextStart              int
	eofCount               int
	isEnd, bufEmpty, init  bool
	err                    error
	isCreatePosFile        bool
	InitialReadPositionEnd bool // deprecated
}

// Stat tail stats information struct
type Stat struct {
	Inode  uint64 `yaml:"Inode"`
	Offset int64  `yaml:"Offset"`
	Size   int64  `yaml:"Size"`
}

// Open file and position files.
func Open(file string, posfile string) (*Tail, error) {
	t := &Tail{
		file:     file,
		posFile:  posfile,
		init:     true,
		bufEmpty: true,
		buf:      make([]byte, DefaultBufSize),
	}

	if t.InitialReadPositionEnd {
		InitialReadPositionEnd = true
	}

	if err := t.openPosFile(); err != nil {
		return t, err
	}

	if err := t.openLogFile(); err != nil {
		return t, err
	}

	if err := t.PositionUpdate(); err != nil {
		return t, err
	}

	_, err := t.fileFd.Seek(t.Stat.Offset, io.SeekStart)
	return t, err
}

func (t *Tail) openPosFile() error {
	if t.posFile == "" {
		return nil
	}

	fd, err := os.OpenFile(t.posFile, os.O_RDWR, 0644)
	if os.IsNotExist(err) {
		fd, err = os.OpenFile(t.posFile, os.O_RDWR|os.O_CREATE, 0644)
		t.isCreatePosFile = true
	}
	if err != nil {
		return err
	}
	t.posFd = fd

	posdata, err := io.ReadAll(fd)
	if err != nil {
		return err
	}

	var posStat Stat
	if err := yaml.Unmarshal(posdata, &posStat); err != nil {
		return err
	}
	t.Stat = posStat
	return nil
}

func (t *Tail) openLogFile() error {
	fd, err := os.Open(t.file)
	if err != nil {
		return err
	}
	t.fileFd = fd

	fdStat, err := fd.Stat()
	if err != nil {
		return err
	}

	stat := fdStat.Sys().(*syscall.Stat_t)
	if stat.Ino == t.Stat.Inode && stat.Size >= t.Stat.Size {
		t.offset1 = t.Stat.Offset
	} else {
		t.Stat.Offset = 0
	}
	t.Stat.Inode = stat.Ino
	t.Stat.Size = stat.Size
	return nil
}

// Close is file and position file close.
func (t *Tail) Close() error {
	if err := t.posFd.Close(); err != nil {
		return err
	}
	return t.fileFd.Close()
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
	if _, err = t.posFd.Write(yml); err != nil {
		return err
	}
	return t.posFd.Sync()
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
		if (InitialReadPositionEnd && t.isCreatePosFile) ||
			(InitialReadPositionEnd && t.posFile == "") {
			t.offset1, _ = t.fileFd.Seek(0, io.SeekEnd)
		}
		t.init = false
	}
}

// Scan is start scan.
func (t *Tail) Scan() bool {
	t.scanInit()
	t.start = t.nextStart

	for {
		if t.bufEmpty {
			t.offset2, _ = t.fileFd.Seek(t.offset1, io.SeekStart)
			t.n, t.err = t.fileFd.Read(t.buf)

			if t.n == 0 || errors.Is(t.err, io.EOF) {
				t.eofCount++
				if t.eofCount > 5 {
					t.eofCount = 0
					t.fileCheck()
					continue
				}
				time.Sleep(ReadLineTimeout / 5)
				continue
			}
			t.bufEmpty = false
		}
		t.eofCount = 0

		for i := t.start; i < t.n; i++ {
			if t.buf[i] == '\n' {
				t.end = i
				t.nextStart = i + 1
				t.isEnd = false
				return true
			}
		}

		t.offset1 += int64(t.end)
		t.bufEmpty = true

		if t.offset1 == t.offset2 {
			if !t.isEnd {
				t.isEnd = true
				time.Sleep(ReadLineTimeout)
				continue
			}
			t.isEnd = false
			t.end = t.n
			t.Stat.Offset = t.offset1 - 1
			t.PositionUpdate()
			return true
		}
		t.offset1++
		t.start, t.end, t.nextStart = 0, 0, 0
	}
}

func (t *Tail) fileCheck() error {
	fdstat, err := t.fileFd.Stat()
	if err != nil {
		return err
	}

	s := fdstat.Sys().(*syscall.Stat_t)
	t.Stat.Inode = s.Ino
	t.Stat.Size = s.Size
	t.Stat.Offset = t.offset1 - 1

	if err := t.PositionUpdate(); err != nil {
		return err
	}

	for {
		fd, err := os.Open(t.file)
		if os.IsNotExist(err) {
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

		if t.Stat.Inode == newStat.Ino && t.Stat.Size == newStat.Size {
			fd.Close()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if t.Stat.Inode != newStat.Ino {
			t.Stat.Inode = newStat.Ino
			t.Stat.Offset, t.offset1 = 0, 0
			t.Stat.Size = newStat.Size
			t.fileFd.Close()
			t.fileFd = fd
			break
		}

		if newStat.Size < t.Stat.Size {
			t.fileFd.Seek(0, io.SeekStart)
			t.Stat.Size = newStat.Size
			fd.Close()
			break
		}

		if newStat.Size > t.Stat.Size {
			t.fileFd.Seek(t.Stat.Offset, io.SeekStart)
			fd.Close()
			break
		}
	}
	return nil
}
