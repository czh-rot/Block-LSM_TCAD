package main

import (
	"Exper"
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"io"
	"os"
	"time"

	//"syndtr/goleveldb/leveldb"
)
var(
	key []byte
	value []byte
	WriteTime float64
	temp []byte
	t3 time.Time
	t4 time.Time
	t float64
	Txc int
)

// 原始写入性能测试函数
func Int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

func main() {
	idle0, total0 := Exper.GetCPUSample()
	db, err := leveldb.OpenFile("path", &opt.Options{
		Compression:			opt.NoCompression, // none,不启用snappy压缩机制，for RW
		WriteBuffer:			128* opt.MiB, // LSM-tree mem1，可用其存顺序数据， for RW
		WriteBuffer2: 			4*opt.MiB, // LSM-tree mem2，可用其存txlookupentry，for RW
		BlockCacheCapacity:		10* opt.MiB, // 块缓存大小, for R
		OpenFilesCacheCapacity: 128, //tablecache大小, for R
		Filter:filter.NewBloomFilter(10), // bf大小，bitsperkey为10, for RW
	})

	if err != nil {
		panic(err)
	}
	defer db.Close()
	fi, err := os.Open("txt location")
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	//defer db2.Close()Double
	defer fi.Close()
	size:=0
	size32:=0
	sizeNo32:=0
	totali:=0
	keyi:=0
	br := bufio.NewReader(fi)
	t1:=time.Now()
	for {
		if totali % 2 == 0 {
			a, c := br.ReadString('\n')
			key, _ = hex.DecodeString(a)
			keyi++
			if c == io.EOF {
				break
			}
		} else {
			a, c := br.ReadString('\n')
			value, _ = hex.DecodeString(a)
			//size += len(value)
			if c == io.EOF {
				break
			}
			_ = db.Put(key, value, nil)
			sizeNo32 += len(key) + len(value)
			//if len(key) == 33 && bytes.Compare(key[:1], []byte("l")) == 0 {
			//	_ = db.Put(key, value, nil)
			//	sizeNo32 += len(key) + len(value)
			//	Txc++
			//}
		}
		totali++
	}
	size = size32 + sizeNo32
	t2:=time.Now()
	db.PrintTime()
	fmt.Println("发生次数:",leveldb.Count, "count: ",Txc)
	fmt.Println("总时间",t2.Sub(t1).Seconds(),"Put时间",WriteTime)
	fmt.Println("总条目数:",totali,"key数目:",keyi)
	fmt.Println("总大小:",size,size32,sizeNo32,size32+sizeNo32)
	f := float64(size / 1024 / 1024)
	fmt.Println("吞吐量为:",float64(f/WriteTime),float64(keyi)/WriteTime)
	fmt.Println(db.GetmemComp())
	fmt.Println(db.Getlevel0Comp())
	fmt.Println(db.Getnonlevel0Comp())

	idle1, total1 := Exper.GetCPUSample()

	idleTicks := float64(idle1 - idle0)
	totalTicks := float64(total1 - total0)
	cpuUsage := 100 * (totalTicks - idleTicks) / totalTicks

	fmt.Printf("CPU usage is %f%% [busy: %f, total: %f]\n", cpuUsage, totalTicks-idleTicks, totalTicks)
}
