# gotail [![GoDoc](https://godoc.org/github.com/masa23/gotail?status.svg)](https://godoc.org/github.com/masa23/gotail) [![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/hyperium/hyper/master/LICENSE)

gotail is Go library for reading data from realtime updating file , read like "tail -f" command.  
See https://godoc.org/github.com/masa23/gotail for the API document.

## License
MIT

## Example

```
// init construct
const (
	LogFile = "./test.log"
	PosFile = "./test.log.pos"
)

func main() {
	go func() {
		fd, err := os.OpenFile(LogFile, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			panic(err)
		}
		defer fd.Close()
		fd.Truncate(0)
		fd.Seek(0, 0)

		for {
			t := time.Now().String()
			fd.WriteString(t + "\n")
			fd.Sync()
			time.Sleep(time.Second)
		}
	}()
	tail, err := gotail.Open(LogFile, PosFile)
	if err != nil {
		panic(err)
	}


	for tail.Scan() {
		b := tail.Text()
		fmt.Println(b)
	}

	if err = tail.Err(); err != nil {
		panic(err)
	}
}
```
