package gotail

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

var tmp string

func TestMain(m *testing.M) {
	var err error
	tmp, err = ioutil.TempDir("", "gotail_test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmp)

	// generate test log file
	go func() {
		path := filepath.Join(tmp, "test.txt")
		fd, err := os.Create(path)
		if err != nil {
			panic(err)
		}
		defer fd.Close()
		i := 0
		for {
			now := time.Now()
			fd.WriteString(strconv.Itoa(i) + " ")
			fd.WriteString(now.Format("random mojiretsu"))
			fd.WriteString("\n")
			i++
		}
	}()
	time.Sleep(time.Millisecond)
	ret := m.Run()
	os.RemoveAll(tmp)
	os.Exit(ret)
}

func TestOpen(t *testing.T) {
	path := filepath.Join(tmp, "test.txt")
	posPath := filepath.Join(tmp, "test.txt.pos")
	_, err := Open(path, posPath)
	if err != nil {
		t.Fatal(err)
	}
}

func TestScan(t *testing.T) {
	path := filepath.Join(tmp, "test.txt")
	tail, err := Open(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer tail.Close()

	if !tail.Scan() {
		t.Fatal(fmt.Errorf("scan error"))
	}
}

func TestBytes(t *testing.T) {
	path := filepath.Join(tmp, "test.txt")
	posPath := filepath.Join(tmp, "test.txt.pos")
	tail, err := Open(path, posPath)
	InitialReadPositionEnd = true
	if err != nil {
		t.Fatal(err)
	}
	defer tail.Close()

	i := 0
	for tail.Scan() {
		if err := tail.Err(); err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(tail.Bytes(), []byte(strconv.Itoa(i)+" random mojiretsu")) {
			t.Fatal(strconv.Itoa(i)+" lines read miss match", "sample:"+strconv.Itoa(i)+" random mojiretsu", "read:"+string(tail.Bytes()))
		}
		if i > 100000 {
			break
		}
		i++
	}

	if err := tail.Err(); err != nil {
		t.Fatal(err)
	}
}
