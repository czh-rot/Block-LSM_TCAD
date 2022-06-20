package main

import (
	"Exper"
	"bufio"
	"bytes"
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
)
// 对 v   于EthData-202w，总条目64894200，键值对数目为32447100，长度为32字节的Key有2343907，确实是1/10阿
// 所有数据字节为4715961535，约4.4G左右，数据比较正确科学。
// E:/record/address.txt 中有一百万个账户，我们用六七十万即可

func Int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

// 这个方法是添加前缀后的测试
func main() {
	// 软件实现CPU利用率，但是这个是全程的Average CPU Usage，所以并不能说明多少问题
	idle0, total0 := Exper.GetCPUSample()
	// Create or Open数据库
	db, err := leveldb.OpenFile("database path", &opt.Options{
		Compression:			opt.NoCompression, // none,不启用snappy压缩机制，for RW
		WriteBuffer:			128* opt.MiB, // LSM-tree mem1，可用其存顺序数据， for RW
		WriteBuffer2: 			4*opt.MiB, // LSM-tree mem2，可用其存txlookupentry，for RW
		BlockCacheCapacity:		10* opt.MiB, // 块缓存大小, for R
		OpenFilesCacheCapacity: 128, //tablecache大小, for R
		Filter:filter.NewBloomFilter(10), // bf大小，bitsperkey为10, for RW
	})
	// 若数据库Create or Open失败，输出error信息
	if err != nil {
		panic(err)
	}
	// 程序结束前关闭数据库
	defer db.Close()
	// 打开txt文件
	fi, err := os.Open("/media/czh/sn/ExpData")
	// 打开文件失败，则报错
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	// 程序结束前关闭打开的文件
	defer fi.Close()
	size:=0 // 记录总数据大小
	size32:=0 // 记录state数据大小
	sizeNo32:=0 // 记录非状态数据大小
	totali:=0 // 记录txt中总共的字符串条目数目
	keyi:=0 // 记录txt中总共的kv对数目
	br := bufio.NewReader(fi)
	t1:=time.Now()
	MPT:=0
	buf := []byte{0,0,0,0}
	for {
		if totali % 2 == 0{
			a,  c := br.ReadString('\n')
			key, _ =hex.DecodeString(a)
			//size += len(key)
			if len(key) == 32{
				MPT++
			}
			keyi++
			if c == io.EOF {
				break
			}
		}else{
			a,  c := br.ReadString('\n')
			value, _ =hex.DecodeString(a)
			//size += len(value)
			if c == io.EOF {
				break
			}
			if len(key) == 32{
				// state data
				key = append(buf[:],key...)
				t3 = time.Now()
				_ = db.Put(key, value, nil)
				t4 = time.Now()
				t = t4.Sub(t3).Seconds()
				//_ = db2.Put(key, value, nil)
				size32 += len(key) + len(value)
			}else {
				if len(key) == 33 && bytes.Compare(key[:1],[]byte("l")) == 0{
					// the kv belongs to lookup tx entry
					//buf = []byte{0,0,0}
					//temp = append([]byte{0},buf[:]...)
					//key = append(temp,key...)
					t3 = time.Now()
					_ = db.Put_s(key, value, nil)
					t4 = time.Now()
					t = t4.Sub(t3).Seconds()
					sizeNo32 += len(key) + len(value)
				}else if len(key) == 33 && bytes.Compare(key[:1],[]byte("H")) == 0{
					buf = value[4:]
					key = append(buf[:],key...)
					t3 = time.Now()
					_ = db.Put(key, value, nil)
					t4 = time.Now()
					t = t4.Sub(t3).Seconds()
					//_ = db2.Put_s(key, value, nil)
					sizeNo32 += len(key) + len(value)
				}else{
					// else non-state data
					key = append(buf[:],key...)
					t3=time.Now()
					_ = db.Put(key, value, nil)
					t4=time.Now()
					t=t4.Sub(t3).Seconds()
					//_ = db2.Put_s(key, value, nil)
					sizeNo32 += len(key) + len(value)
				}
			}
			WriteTime += t
		}
		totali++
	}
	size = size32 + sizeNo32
	t2:=time.Now()
	db.PrintTime()
	fmt.Println("发生次数:",leveldb.Count) // 强行执行minor compaction的次数
	fmt.Println("总时间",t2.Sub(t1).Seconds(),"Put时间",WriteTime) // 前者为程序运行总时间，后者为数据写的总时间，我们使用后者
	fmt.Println("总条目数:",totali,"key数目:",keyi,"MPT数目:",MPT) // 分别为总条目、kv对数目，以及MPT中KV对的数目
	fmt.Println("总大小:",size,size32,sizeNo32,size32+sizeNo32)
	f := float64(size / 1024 / 1024)
	fmt.Println("吞吐量为:",float64(f/WriteTime),float64(keyi)/WriteTime) // TPS，前者为MB/s,后者为TPS
	fmt.Println(db.GetmemComp()) // minor compaction
	fmt.Println(db.Getlevel0Comp()) // major c in l0
	fmt.Println(db.Getnonlevel0Comp()) // major c in ln

	idle1, total1 := Exper.GetCPUSample()

	idleTicks := float64(idle1 - idle0)
	totalTicks := float64(total1 - total0)
	cpuUsage := 100 * (totalTicks - idleTicks) / totalTicks

	fmt.Printf("CPU usage is %f%% [busy: %f, total: %f]\n", cpuUsage, totalTicks-idleTicks, totalTicks)
}
