package Read

import (
	"MPT/ethdb"
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	//"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	//"github.com/syndtr/goleveldb/leveldb/cache"
	_ "github.com/syndtr/goleveldb/leveldb/opt"
	_ "io"
	log2 "log"
	"math/big"
	"math/rand"
	_ "math/rand"
	"os"
	"runtime"
	//"strconv"
	"testing"
	"time"
)

var (
	// databaseVerisionKey tracks the current database version.
	databaseVerisionKey = []byte("DatabaseVersion")

	// headHeaderKey tracks the latest known header's hash.
	headHeaderKey = []byte("LastHeader")

	// headBlockKey tracks the latest known full block's hash.
	headBlockKey = []byte("LastBlock")

	// headFastBlockKey tracks the latest known incomplete block's hash during fast sync.
	headFastBlockKey = []byte("LastFast")

	// fastTrieProgressKey tracks the number of trie entries imported during fast sync.
	fastTrieProgressKey = []byte("TrieSync")

	// snapshotRootKey tracks the hash of the last snapshot.
	snapshotRootKey = []byte("SnapshotRoot")

	// snapshotJournalKey tracks the in-memory diff layers across restarts.
	snapshotJournalKey = []byte("SnapshotJournal")

	// txIndexTailKey tracks the oldest block whose transactions have been indexed.
	txIndexTailKey = []byte("TransactionIndexTail")

	// fastTxLookupLimitKey tracks the transaction lookup limit during fast sync.
	fastTxLookupLimitKey = []byte("FastTransactionLookupLimit")

	// Data item prefixes (use single byte to avoid mixing data types, avoid `i`, used for indexes).
	headerPrefix       = []byte("h") // headerPrefix + num (uint64 big endian) + hash -> header
	headerTDSuffix     = []byte("t") // headerPrefix + num (uint64 big endian) + hash + headerTDSuffix -> td
	headerHashSuffix   = []byte("n") // headerPrefix + num (uint64 big endian) + headerHashSuffix -> hash
	headerNumberPrefix = []byte("H") // headerNumberPrefix + hash -> num (uint64 big endian)

	blockBodyPrefix     = []byte("b") // blockBodyPrefix + num (uint64 big endian) + hash -> block body
	blockReceiptsPrefix = []byte("r") // blockReceiptsPrefix + num (uint64 big endian) + hash -> block receipts

	txLookupPrefix        = []byte("l") // txLookupPrefix + hash -> transaction/receipt lookup metadata
	bloomBitsPrefix       = []byte("B") // bloomBitsPrefix + bit (uint16 big endian) + section (uint64 big endian) + hash -> bloom bits
	SnapshotAccountPrefix = []byte("a") // SnapshotAccountPrefix + account hash -> account trie value
	SnapshotStoragePrefix = []byte("o") // SnapshotStoragePrefix + account hash + storage hash -> storage trie value

	preimagePrefix = []byte("secure-key-")      // preimagePrefix + hash -> preimage
	configPrefix   = []byte("ethereum-config-") // config prefix for the db

	// Chain index prefixes (use `i` + single byte to avoid mixing data types).
	BloomBitsIndexPrefix = []byte("iB") // BloomBitsIndexPrefix is the data table of a chain indexer to track its progress

	Count int
	tt1 float64
	tt2 float64
	tt3 float64
	tx []byte
	ac []byte
)
type LegacyTxLookupEntry struct {
	BlockHash  common.Hash
	BlockIndex uint64
	Index      uint64
}

func BytesToHash(b []byte) common.Hash {
	var h common.Hash
	h.SetBytes(b)
	return h
}

func headerNumberKey(hash common.Hash) []byte {
	return append(headerNumberPrefix, hash.Bytes()...)
}

func ReadHeaderNumber(db ethdb.LDBDatabase, hash common.Hash) uint64 {
	data, _ := db.Get(headerNumberKey(hash))
	if len(data) != 8 {
		return 0
	}
	number := binary.BigEndian.Uint64(data)
	return number
}

func GetTxLookupEntry(db ethdb.LDBDatabase, hash common.Hash) uint64 {
	// Load the positional metadata from disk and bail if it fails
	data, _ := db.Get(append(txLookupPrefix, hash.Bytes()...))
	if len(data) == 0 {
		return 0
	}
	if len(data) < common.HashLength {
		number := new(big.Int).SetBytes(data).Uint64()
		return number
	}
	if len(data) == common.HashLength {
		return ReadHeaderNumber(db, common.BytesToHash(data))
	}
	// Finally try database v3 tx lookup format
	var entry LegacyTxLookupEntry
	if err := rlp.DecodeBytes(data, &entry); err != nil {
		log.Error("Invalid transaction lookup entry RLP", "hash", hash, "blob", data, "err", err)
		return 0
	}
	return 0
}

func GetTxLookupEntry_s(db ethdb.LDBDatabase, hash common.Hash) uint64 {
	// Load the positional metadata from disk and bail if it fails
	data, _ := db.Get_s(append(txLookupPrefix, hash.Bytes()...))
	if len(data) == 0 {
		return 0
	}
	if len(data) < common.HashLength {
		number := new(big.Int).SetBytes(data).Uint64()
		return number
	}
	if len(data) == common.HashLength {
		return ReadHeaderNumber(db, common.BytesToHash(data))
	}
	// Finally try database v3 tx lookup format
	var entry LegacyTxLookupEntry
	if err := rlp.DecodeBytes(data, &entry); err != nil {
		log.Error("Invalid transaction lookup entry RLP", "hash", hash, "blob", data, "err", err)
		return 0
	}
	return 0
}

func headerKey(hash common.Hash, number uint64) []byte {
	return append(append(headerPrefix, encodeBlockNumber(number)...), hash.Bytes()...)
}

func hashKey(number uint64) []byte {
	return append(append(headerPrefix,encodeBlockNumber(number)...),headerHashSuffix...)
}

func Block_hashkey(number uint64) []byte {
	prefix := encodeBlockNumber(number)
	//fmt.Println(prefix)
	return append(append(append(prefix[5:], headerPrefix...), encodeBlockNumber(number)...),headerHashSuffix...)
}

func encodeBlockNumber(number uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, number)
	return enc
}

func blockBodyKey(hash common.Hash, number uint64) []byte {
	return append(append(blockBodyPrefix, encodeBlockNumber(number)...), hash.Bytes()...)
}

func Block_blockBodyKey(hash common.Hash, number uint64) []byte {
	prefix := encodeBlockNumber(number-1)
	return append(append(append(prefix[5:], blockBodyPrefix...), encodeBlockNumber(number)...), hash.Bytes()...)
}

func GetBodyRLP(db ethdb.LDBDatabase, hash common.Hash, number uint64) rlp.RawValue {
	data, _ := db.Get(blockBodyKey(hash, number))
	return data
}

func Block_GetBodyRLP(db ethdb.LDBDatabase, hash common.Hash, number uint64) rlp.RawValue {
	data, _ := db.Get(Block_blockBodyKey(hash, number))
	return data
}

func GetBodyRLP_s(db ethdb.LDBDatabase, hash common.Hash, number uint64) rlp.RawValue {
	data, _ := db.Get_s(blockBodyKey(hash, number))
	return data
}

func GetBody(db ethdb.LDBDatabase, hash common.Hash, number uint64) *types.Body {
	data := GetBodyRLP(db, hash, number)
	//fmt.Println("Body",data)
	if len(data) == 0 {
		return nil
	}
	body := new(types.Body)
	if err := rlp.Decode(bytes.NewReader(data), body); err != nil {
		log.Error("Invalid block body RLP", "hash", hash, "err", err)
		return nil
	}
	return body
}

func Block_GetBody(db ethdb.LDBDatabase, hash common.Hash, number uint64) *types.Body {
	data := Block_GetBodyRLP(db, hash, number)
	//fmt.Println("Body",data)
	if len(data) == 0 {
		return nil
	}
	body := new(types.Body)
	if err := rlp.Decode(bytes.NewReader(data), body); err != nil {
		log.Error("Invalid block body RLP", "hash", hash, "err", err)
		return nil
	}
	return body
}

func GetBody_s(db ethdb.LDBDatabase, hash common.Hash, number uint64) *types.Body {
	data := GetBodyRLP_s(db, hash, number)
	//fmt.Println("Body",data)
	if len(data) == 0 {
		return nil
	}
	body := new(types.Body)
	if err := rlp.Decode(bytes.NewReader(data), body); err != nil {
		log.Error("Invalid block body RLP", "hash", hash, "err", err)
		return nil
	}
	return body
}


func GetTransaction(db ethdb.LDBDatabase, hash common.Hash) (*types.Transaction, common.Hash, uint64, uint64) {
	// Retrieve the lookup metadata and resolve the transaction from the body
	// 取出区块号
	t1:=time.Now()
	blockNumber:= GetTxLookupEntry(db, hash)
	t2:=time.Now()
	tt1 += t2.Sub(t1).Seconds()
	//fmt.Print("t1:", tt1)

	headerhash:=hashKey(blockNumber) // prefix + num + suffix --> hash
	// 取区块hash
	t3:=time.Now()
	blkhash3, _ :=db.Get(headerhash)
	t4:=time.Now()
	tt2 += t4.Sub(t3).Seconds()
	//fmt.Print(" t2:", tt2)

	body := GetBody(db,BytesToHash(blkhash3),blockNumber) // b + num + hash --> body
	t5:=time.Now()
	tt3 += t5.Sub(t4).Seconds()
	//fmt.Println(" t3:", tt3)

	if body == nil{
		Count++
	}
	// serliazaiton,memory body data, tx list,
	return nil, common.Hash{}, 0, 0
}

func GetTransaction_s(db ethdb.LDBDatabase, hash common.Hash) (*types.Transaction, common.Hash, uint64, uint64) {
	// Retrieve the lookup metadata and resolve the transaction from the body
	// 取出区块号
	t1:=time.Now()
	blockNumber:= GetTxLookupEntry_s(db, hash)
	t2:=time.Now()
	tt1 += t2.Sub(t1).Seconds()
	//fmt.Print("t1:", tt1)

	headerhash:=hashKey(blockNumber) // prefix + num + suffix --> hash
	// 取区块hash
	t3:=time.Now()
	blkhash3, _ :=db.Get_s(headerhash)
	t4:=time.Now()
	tt2 += t4.Sub(t3).Seconds()
	//fmt.Print(" t2:", tt2)

	body := GetBody_s(db,BytesToHash(blkhash3),blockNumber) // b + num + hash --> body
	t5:=time.Now()
	tt3 += t5.Sub(t4).Seconds()
	//fmt.Println(" t3:", tt3)

	if body == nil{
		Count++
	}
	// serliazaiton,memory body data, tx list,
	return nil, common.Hash{}, 0, 0
}

func Block_GetTransaction(db ethdb.LDBDatabase, hash common.Hash) (*types.Transaction, common.Hash, uint64, uint64) {
	// Retrieve the lookup metadata and resolve the transaction from the body
	// 取出区块号
	t1:=time.Now()
	blockNumber:= GetTxLookupEntry_s(db, hash)
	//fmt.Println(blockNumber)
	t2:=time.Now()
	tt1 += t2.Sub(t1).Seconds()

	headerhash := Block_hashkey(blockNumber) // prefix + num + suffix --> hash
	// 取区块hash
	//fmt.Println(headerhash)
	t3:=time.Now()
	blkhash3, _ :=db.Get(headerhash)
	//fmt.Println(blkhash3)
	t4:=time.Now()
	tt2 += t4.Sub(t3).Seconds()
	body := Block_GetBody(db,BytesToHash(blkhash3),blockNumber) // b + num + hash --> body
	t5:=time.Now()
	tt3 += t5.Sub(t4).Seconds()

	if body == nil{
		Count++
	}

	return nil, common.Hash{}, 0, 0
}

func GetHeaderRLP(db ethdb.LDBDatabase, hash common.Hash, number uint64) rlp.RawValue {
	data, _ := db.Get(headerKey(hash, number))
	return data
}

func GetHeader(db ethdb.LDBDatabase, hash common.Hash, number uint64) *types.Header {
	data := GetHeaderRLP(db, hash, number)
	if len(data) == 0 {
		return nil
	}
	header := new(types.Header)
	if err := rlp.Decode(bytes.NewReader(data), header); err != nil {
		log2.Println("Error")
		return nil
	}
	return header
}

var(
	Txhash [10000000][]byte //长度代表你想要读取的交易的set数目
	Acchash [10000000][]byte
	LoopCnt int //循环次数
)

func TestObtainAllMPTroot(t *testing.T) {
	db,_ := ethdb.NewLDBDatabase("path",16,1024)
	defer db.Close()

	// record all mpt root
	fr, err2 := os.OpenFile("path",os.O_CREATE | os.O_APPEND |os.O_WRONLY, 0660)
	if err2 != nil {
		log2.Println("Error")
	}
	defer fr.Close()

	number := 2000000 //从200万开始
	blockheight := 6000000
	for i := number; i < blockheight; i++ {
		hashkey := append(append(headerPrefix, encodeBlockNumber(uint64(i))...), headerHashSuffix...)
		// qukuaide hash
		hash, _ := db.Get(hashkey)
		header := GetHeader(*db, common.BytesToHash(hash), uint64(i))
		MPTroot := header.Root.Bytes()
		fmt.Fprintln(fr, hex.EncodeToString(MPTroot))
	}
}

func block_hashkey2(l uint64, number uint64) []byte{
	prefix := encodeBlockNumber(l)
	return append(append(append(prefix[5:],headerPrefix...), encodeBlockNumber(number)...), headerHashSuffix...)
}

var l uint64
var blockNumber uint64
func TestNumber(t *testing.T)  {
	db,_ := ethdb.NewLDBDatabase("/media/czh/sn2/200G_P",400,1024)
	blockNumber = 4727500
	for l = 0; l < 5400000 ; l++{
		headerHash := block_hashkey2(l, blockNumber)
		data,_ := db.Get(headerHash)
		if data!=nil {
			fmt.Println(data)
			fmt.Println(l)
		}
	}
}

// WRITE：需要主要的是
// 如果在init design上进行write的验证，需要使用
// Put_s来存储所有的数据
// 如果在optimization上进行Write的实验，需要使用
// Put来写带前缀的，Put_s来写不带前缀的

// READ：需要注意的是
// 如果在init design上进行read的验证，需要使用
// GetTransaction_s来读取交易、使用Get_s来读取账户
// 如果在optimization上进行Read的实验，需要使用
// Block_GetTransaction来读取交易、使用Get来读取账户

func TestTransactionRandom(t *testing.T){
	// Note the size of the cache and handles
	db,_ := ethdb.NewLDBDatabase("/media/czh/sn2/200G_P",400,1024)

	f1,_:=os.Open("/media/czh/sn2/Tx.txt")
	s1 := bufio.NewScanner(f1)
	Txnumber := 0 // 记录txt中当前遍历到的tx的数目
	Count_T := 0 // 记录当前持有的tx的数目
	for s1.Scan() {
		str:=s1.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		if Txnumber % 19 == 0{
			Txhash[Count_T] = key
			Count_T++
		}
		Txnumber++
		if Count_T == 10000000{ //保存了10100000个交易
			break
		}
	}
	_ = f1.Close()
	fmt.Println("==============")
	number := 0
	rand.Seed(0)
	LoopCnt = 10000000
	for i := 0; i < LoopCnt; i++{
		j := rand.Intn(10000000)
		//j:=5284409
		tx = Txhash[j]
		txh := BytesToHash(tx)
		//_, _, _, _ = GetTransaction_s(*db, txh)
		_, _, _, _ = Block_GetTransaction(*db, txh)
		number++
		if i % 1000000 == 0{
			log2.Println(i)
		}
	}
	fmt.Println(tt1, tt2, tt3)
	tt := tt1 + tt2 + tt3
	fmt.Println(Count)
	fmt.Println(tt)
	runtime.GC()
}

func TestAccountRandom(t *testing.T){
	// Note the size of the cache and handles
	db,_ := ethdb.NewLDBDatabase("/media/czh/sn2/200G",400,1024)

	f1,_:=os.Open("/media/czh/sn/AccKV.txt")
	s1 := bufio.NewScanner(f1)
	Accnumber := 0 // 记录txt中当前遍历到的tx的数目
	Count_Acc := 0 // 记录当前持有的tx的数目
	for s1.Scan() {
		str:=s1.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		if Accnumber % 50 == 0{
			Acchash[Count_Acc] = key
			Count_Acc++
		}
		Accnumber++
		if Count_Acc == 10000000{ //保存了10100000个交易
			break
		}
	}
	_ = f1.Close()
	fmt.Println("==============")
	number := 0
	rand.Seed(0)
	LoopCnt = 1000000
	var total_st float64
	for i := 0; i < LoopCnt; i++{
		// 假设10个kv对为1次read account
		st1 := time.Now()
		for j := 0; j < 10; j++ {
			m := rand.Intn(10000000)
			ac = Acchash[m]
			db.Get(ac)
			number++
		}
		st2 := time.Now()
		total_st += st2.sub(st1).Seconds()
		if i % 1000000 == 0{
			log2.Println(i)
		}
	}
	fmt.Println(Count)
	fmt.Println("TPS: ", total_st/float64(LoopCnt))
	runtime.GC()
}

func TestMixRandom(t *testing.T){
	// Note the size of the cache and handles
	db,_ := ethdb.NewLDBDatabase("database",16,1)

	f2,_:=os.Open("/media/czh/sn2/AccKV.txt")
	s2 := bufio.NewScanner(f2)
	Accnumber := 0 // 记录txt中当前遍历到的tx的数目
	Count_Acc := 0 // 记录当前持有的tx的数目
	for s2.Scan() {
		str:=s2.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		if Accnumber % 7 == 0{
			Acchash[Count_Acc] = key
			Count_Acc++
		}
		Accnumber++
		if Count_Acc == 10000000{ //保存了10100000个交易
			break
		}
	}
	_ = f2.Close()
	f1,_:=os.Open("/media/czh/sn2/Tx.txt")
	s1 := bufio.NewScanner(f1)
	Txnumber := 0 // 记录txt中当前遍历到的tx的数目
	Count_T := 0 // 记录当前持有的tx的数目
	for s1.Scan() {
		str:=s1.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		if Txnumber % 7 == 0{
			Txhash[Count_T] = key
			Count_T++
		}
		Txnumber++
		if Count_T == 10000000{ //保存了10100000个交易
			break
		}
	}
	_ = f1.Close()
	fmt.Println("==============")
	number := 0
	rand.Seed(0)
	LoopCnt = 1000000
	for i := 0; i < LoopCnt; i++{
		// ratio为tx和acc的比例，当ratio为3时，tx：acc = 3 ：7
		ratio := 3
		for r := 0; r < ratio; r++ {
			rtx:= rand.Intn(10000000)
			tx = Txhash[rtx]
			txh := BytesToHash(tx)
			_, _, _, _ = GetTransaction_s(*db, txh)
			number++
		}
		for r:= 10; r >= ratio; r-- {
			// 假设10个kv对为1次read account
			for j := 0; j < 10; j++ {
				racc := rand.Intn(10000000)
				ac = Acchash[racc]
				db.Get(ac)
				number++
			}
		}
		if i % 1000000 == 0{
			log2.Println(i)
		}
	}
	fmt.Println(Count)
	runtime.GC()
}



