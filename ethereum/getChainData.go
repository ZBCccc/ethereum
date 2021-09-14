package main

import (
	"bufio"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
)

var (
	dbPath      = "E:/Geth/geth/chaindata"
	ancientPath = dbPath + "/ancient"
	blocknum    uint64
)

func main() {
	// 设置 CPU 核数,默认全开
	// numcpu := runtime.NumCPU()
	// runtime.GOMAXPROCS(numcpu)

	// 开启一个线程接收键盘命令
	//SetupCloseHandler()

	// 打开ancient文件
	block_1, err := os.OpenFile("data/block_1.csv", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	block_2, err := os.OpenFile("data/block_2.csv", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	defer block_1.Close()
	writer01 := csv.NewWriter(block_1)
	writer02 := csv.NewWriter(block_2)
	// blockNumber 和 timeStamp 等价
	var table = []string{"blockNumber", "hash", "from", "nonce", "to", "value", "gas", "gasPrice", "transactionfee", "input", "type"}
	err = writer01.Write(table) 	//写入头部信息
	err = writer02.Write(table) 	//写入头部信息

	if err != nil {
		panic(err)
	}
	writer01.Flush()
	writer02.Flush()

	ancientDb, err := rawdb.NewLevelDBDatabaseWithFreezer(dbPath, 16, 3, ancientPath, "", true)
	// ancientDb, err := rawdb.NewLevelDBDatabase(dbPath, 16, 1, "", true)

	if err != nil {
		panic(err)
	}

	//startnum := Getstartnum()
	//var endnum uint64 = 13100000

	// 建立通信管道
	//writeChan01 := make(chan types.Transactions, 10000)	// 写管道
	writeChan02 := make(chan types.Transactions, 10000)	// 写管道
	//readChan := make(chan types.Transactions, 10000)		// 读管道
	//finishChan := make(chan bool, 2)	//判断是否读完的管道
	exitChan := make(chan bool, 2)	//判断是否读完的管道

	starttime := time.Now()

	// 开启双协程
	//go GetTxsList(ancientDb, 90000, 150000,writeChan01)
	go GetTxsList(ancientDb, 150001, 210000,writeChan02)

	//go TxsProc(writeChan01, exitChan, writer01)
	go TxsProc(writeChan02, exitChan, writer02)

	for i := 0; i < 1; i++ {
		<- exitChan
	}
	close(exitChan)
	endtime := time.Since(starttime)
	fmt.Println("该函数执行完成耗时：", endtime)
}

// GetTxsList 从数据库中读取数据
func GetTxsList(ancientDb ethdb.Database, istart uint64, iend uint64, writeChan chan types.Transactions/*, finish chan bool*/) {
	var blockheaderhash_b []byte
	var blockheaderhash common.Hash

	//fmt.Println("线程读正在运行...")
	for blocknum = istart; blocknum <= iend; blocknum++ {
		// 只能访问 ancient 数据
		blockheaderhash_b, _ = ancientDb.Ancient("hashes", blocknum)

		blockheaderhash.SetBytes(blockheaderhash_b)
		Block := rawdb.ReadBlock(ancientDb, blockheaderhash, blocknum)

		if Block == nil {
			// 只能访问 leveldb 数据库
			var headerPrefix = []byte("h") // headerPrefix + num (uint64 big endian) + hash -> header
			var numSuffix = []byte("n")    // headerPrefix + num (uint64 big endian) + numSuffix -> hash
			blocknum_b := make([]byte, 8)
			binary.BigEndian.PutUint64(blocknum_b, uint64(blocknum))
			blocknum_key := append(headerPrefix, blocknum_b...)
			blocknum_key = append(blocknum_key, numSuffix...)
			blockheaderhash_b, _ = ancientDb.Get(blocknum_key)
			blockheaderhash.SetBytes(blockheaderhash_b)
			Block = rawdb.ReadBlock(ancientDb, blockheaderhash, blocknum)
		}

		txslist := Block.Transactions()
		writeChan<- txslist
	}
	close(writeChan)
	//finish<- true

}

var (
	To      string
	From    string
	Hash    string
	tx      []string
	chainid = big.NewInt(1)
)

// TxsProc 记录数据到csv文件中
func TxsProc(writeChan chan types.Transactions, exitChan chan bool, writer *csv.Writer) {
	//fmt.Println("线程写正在运行...")
	for {
		txslist, ok := <-writeChan
		if !ok {
			break
		}
		num := txslist.Len()
		var Txs [][]string
		singer := types.NewLondonSigner(chainid)
		for i := 0; i < num; i++ {
			Hash = txslist[i].Hash().String()
			Type := txslist[i].Type()
			AccessList := txslist[i].AccessList()
			_ = AccessList
			Data := txslist[i].Data()
			Gas := txslist[i].Gas()
			GasPrice := txslist[i].GasPrice()
			Value := txslist[i].Value()
			Nonce := txslist[i].Nonce()

			to := txslist[i].To()
			if to != nil {
				To = to.String()
			}

			from, _ := types.Sender(singer, txslist[i])
			From = from.String()

			tx = []string{strconv.FormatUint(blocknum, 10), Hash, From, strconv.FormatUint(Nonce, 10), To, Value.String(), strconv.FormatUint(Gas, 10),
				GasPrice.String(), strconv.FormatUint(Gas*GasPrice.Uint64(), 10), hex.EncodeToString(Data), strconv.Itoa(int(Type))}
			Txs = append(Txs, tx)
			//println("Hash=", Hash)
		}
		err := writer.WriteAll(Txs)
		if err != nil {
			return 
		}
		//fmt.Println("读取到数据...")
	}
	exitChan<- true
}

func SetupCloseHandler() {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\r- Ctrl+C pressed in Terminal")
		LogHeight()
		os.Exit(0)
	}()
}

func LogHeight() {
	log, err := os.OpenFile("data/log.txt", os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("#######################################")
		fmt.Printf("WARNNING !!! 写入日志失败,当前处理区块高度为 %d WARNNING !!!", blocknum)
		fmt.Println("#######################################")
		return
	}
	defer log.Close()

	writer := bufio.NewWriter(log)
	writer.WriteString(strconv.FormatUint(blocknum, 10))
	writer.Flush()
}

func Getstartnum() uint64 {
	log, err := os.OpenFile("data/log.txt", os.O_RDONLY, 0666)
	if err != nil {
		fmt.Println("###########################")
		fmt.Printf("WARNNING !!! 读取日志失败 WARNNING !!!")
		fmt.Println("###########################")
		os.Exit(1)
	}
	defer log.Close()

	reader := bufio.NewReader(log)
	startnum_str, _ := reader.ReadString('\n')
	startnum, _ := strconv.Atoi(startnum_str)
	return uint64(startnum)
}
