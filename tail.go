package tail

import (
	"bufio"
	"io/ioutil"
	"os"
	"syscall"
	"time"

	yaml "gopkg.in/yaml.v2"
)

// Tail メソッド用
type Tail struct {
	file    string
	fileFd  *os.File
	posFile string
	posFd   *os.File
	Stat    Stat
	data    chan []byte
}

// Stat ファイルの状態用の構造体
type Stat struct {
	Ino    uint64 `yaml:"Ino"`
	Offset int64  `yaml:"Offset"`
	Size   int64  `yaml:"Size"`
}

func Open(file string, posfile string) (*Tail, error) {
	var err error
	t := Tail{file: file, posFile: posfile}

	// ポジションファイル読み込み
	t.posFd, err = os.OpenFile(t.posFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return &t, err
	}
	posdata, err := ioutil.ReadAll(t.posFd)
	if err != nil {
		return &t, err
	}
	posStat := Stat{}
	yaml.Unmarshal(posdata, &posStat)

	// tailするファイルを開く
	t.fileFd, err = os.Open(t.file)
	if err != nil {
		return &t, err
	}

	// tailするファイルのstatを取得
	fdStat, err := t.fileFd.Stat()
	if err != nil {
		return &t, err
	}
	stat := fdStat.Sys().(*syscall.Stat_t)

	// statの作成
	t.Stat.Ino = stat.Ino
	t.Stat.Size = stat.Size
	if stat.Ino == posStat.Ino && stat.Size >= posStat.Size {
		// inodeの変更がなくサイズ以前より大きい場合は
		// posのOffsetの続きから読み込む
		t.Stat.Offset = posStat.Offset
	} else {
		// ファイルが変わっている、サイズが小さい場合は
		// Offsetを0にして最初から読み込む
		t.Stat.Offset = 0
	}

	// posファイルの更新
	err = posUpdate(&t)
	if err != nil {
		return &t, err
	}

	// tail 読み込み位置移動
	t.fileFd.Seek(t.Stat.Offset, os.SEEK_SET)

	return &t, nil
}

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

func (t *Tail) TailBytes() []byte {
	return <-t.data
}

func (t *Tail) TailString() string {
	return string(<-t.data)
}

func (t *Tail) Scan() {
	t.data = make(chan []byte)
	go func() {
		var err error
		for {
			scanner := bufio.NewScanner(t.fileFd)
			for scanner.Scan() {
				t.data <- scanner.Bytes()
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
			if stat.Ino != t.Stat.Ino {
				t.Stat.Ino = stat.Ino
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
