package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"log"
	"os"
)

func main() {
	fs, err := os.Open("")
	if err != nil {
		log.Println("Error")
	}
	defer fs.Close()

	fo, err2 := os.OpenFile("E:/DB/200/Key",os.O_CREATE | os.O_APPEND |os.O_WRONLY, 0660)
	if err2 != nil {
		log.Println("Error")
	}
	defer fo.Close()

	s1 := bufio.NewScanner(fs)
	seq := 0
	cnt := 0
	//Isstate := false
	for s1.Scan() {
		str := s1.Text()
		data, _ := hex.DecodeString(str[:])
		if seq % 2 == 0 {
			// key
			if len(data) == 36 {
				//Isstate = true
				cnt++
				fmt.Fprintln(fo, hex.EncodeToString(data))
			}
		}
		//else
		//{
		//	// value
		//	if Isstate == true {
		//		fmt.Fprintln(fo, hex.EncodeToString(data))
		//	}
		//}
		seq++
		//Isstate = false
	}
	log.Println("The number of the state KV items is:", cnt)
}
