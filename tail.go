package gotail

import (
	"bufio"
	"io/ioutil"
	"os"
	"syscall"
	"time"

	"gopkg.in/yaml.v2"
)

// Tail is tail file struct
type Tail struct {
	file                   string
	fileFd                 *os.File
	posFile                string
	posFd                  *os.File
	Stat                   Stat
	data                   []byte
	init                   bool
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
	err = t.PositionUpdate()
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
func (t *Tail) PositionUpdate() error {
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
	return t.data
}

// Text is get one line strings.
func (t *Tail) Text() string {
	return string(t.data)
}

// Err is get Scan error
func (t *Tail) Err() error {
	return t.err
}

// Scan is start scan.
func (t *Tail) Scan() bool {
	if t.init {
		// there is no pos file Start reading from the end of the file
		if (t.InitialReadPositionEnd && t.isCreatePosFile) ||
			(t.InitialReadPositionEnd && t.posFile == "") {
			t.fileFd.Seek(0, os.SEEK_END)
		}
		t.scanner = bufio.NewScanner(t.fileFd)
		t.init = false
	}

	for {
		if t.scanner.Scan() {
			t.data = t.scanner.Bytes()
			return true
		}

		if err := t.scanner.Err(); err != nil {
			t.err = err
			return false
		}

		// ステータスをアップデート
		fdstat, err := t.fileFd.Stat()
		if err != nil {
			t.err = err
			return false
		}
		s := fdstat.Sys().(*syscall.Stat_t)
		t.Stat.Inode = s.Ino
		t.Stat.Size = s.Size
		t.Stat.Offset, err = t.fileFd.Seek(0, os.SEEK_CUR)

		// posファイルアップデート
		err = t.PositionUpdate()
		if err != nil {
			t.err = err
			return false
		}

		for {
			// ファイルを開く
			fd, err := os.Open(t.file)
			if os.IsNotExist(err) {
				//ファイルがない場合は待つ
				time.Sleep(time.Second)
				continue
			} else if err != nil {
				t.err = err
				return false
			}
			newFdStat, err := fd.Stat()
			if err != nil {
				t.err = err
				return false
			}
			newStat := newFdStat.Sys().(*syscall.Stat_t)

			// 変更がない場合待って再度チェックする
			if t.Stat.Inode == newStat.Ino && t.Stat.Size == newStat.Size {
				fd.Close()
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// inodeの変更があったら新しいファイルに差し替える
			if t.Stat.Inode != newStat.Ino {
				t.Stat.Inode = newStat.Ino
				t.Stat.Offset = 0
				t.Stat.Size = newStat.Size
				t.fileFd.Close()
				t.fileFd = fd
				t.scanner = bufio.NewScanner(t.fileFd)
				break
			}

			// サイズ小さくなっていたらSEEK位置を先頭の戻す
			if newStat.Size < t.Stat.Size {
				_, err = t.fileFd.Seek(0, os.SEEK_SET)
				if err != nil {
					t.err = err
					return false
				}
				t.Stat.Size = newStat.Size
				t.scanner = bufio.NewScanner(t.fileFd)
				fd.Close()
				break
			}

			if newStat.Size > t.Stat.Size {
				_, err := t.fileFd.Seek(t.Stat.Offset, os.SEEK_SET)
				if err != nil {
					t.err = err
					return false
				}
				t.scanner = bufio.NewScanner(t.fileFd)
				fd.Close()
				break
			}
		}
	}
}
