package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

type catecount struct {
	cate  string
	count int
}

type catedetail struct {
	cate   string
	detail map[string]int
}

func walkFiles(done <-chan struct{}, root string) (<-chan string, <-chan error) {
	/*
	   遍历目录，每当找到合适的文件，发送到 channel 中，单 goroutine 运行
	*/
	files := make(chan string)
	errc := make(chan error, 1)
	go func() {
		defer close(files) // 无论是正常结束还是从 done 中读取到异常退出信息，都会最终关掉 files channel，防止死锁阻塞
		errc <- filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			if filepath.Ext(path) != ".seg" {
				return nil
			}
			select {
			case files <- path:
			case <-done:
				return errors.New("walk cancelled")
			}
			return nil
		})
	}()
	return files, errc
}

func process_sentence(numWorkers uint32, done <-chan struct{}, files <-chan string, c map[uint32](chan<- catecount), l map[uint32](chan<- catedetail)) {
	/*
	   这个函数会被多个 goroutine 运行，故此函数中没有 close channel l & c
	   每读到一个句子，读出已经分好的词，做统计，再把文件中的总词数，以及每个词的数量分别发送到对应桶的 c & l
	*/
	for path := range files {
		cntWhole := 0
		cntTerm := make(map[string]int)
		lines, err := LoadFileToStrings(path)
		CheckError(err)
		for line := range lines {
			for term := range strings.Split(line, " ") {
				cntTerm[term] += 1
			}
		}
		for t, c := range cntTerm {
			cntWhole += c
		}
		b := GetBucket(path, numWorkers)
		select {
		case c[b] <- catecount{path, cntWhole}:
		case <-done:
			return
		}
		select {
		case l[b] <- catedetail{path, cntTerm}:
		case <-done:
			return
		}
	}
}

func CountFiles(maxlength int, root string, numWorkers int) (map[string]*Word, int) {
	done := make(chan struct{})
	defer close(done) // 如果程序退出，那么也会关掉 done channel，进而各个 goroutine 就得到异常退出的信息，会各自退出

	// 单 goroutine 中运行 walkFiles
	files, errc := walkFiles(done, root)

	// 分别初始化 numWorkers 个 count channels 和 detail channal
	c := map[uint32](chan<- catecount){}
	for i := 0; i < numWorkers; i++ {
		c[uint32(i)] = make(chan<- catecount)
	}
	l := map[uint32](chan<- catedetail){}
	for i := 0; i < numWorkers; i++ {
		l[uint32(i)] = make(chan<- catedetail)
	}
	var wg sync.WaitGroup
	// numWorkers 个 goroutine 运行 process_sentence 函数，于是 files, c, l 几个 channel 都在 goroutine 中形成一个工作流运行
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			process_sentence(uint32(numWorkers), done, files, c, l)
			wg.Done()
		}()
	}
	// 另起一个 goroutine，阻塞在后台，等待 sentence, c, l 的处理 goroutines 都运行结束，无论是正常结束还是因为主进程异常退出结束
	// 这个同步阻塞的目的仅仅是为了最终能够关闭 c & l goroutine，并不是在主线程中阻塞住，等待 c, l 被塞满
	// 故此，使用 goroutine 等待，而不要阻塞主进程。这里如果阻塞了，那么会死锁，因为到这里还没有定义接收 c, l channels 的流程
	// 而 c & l 都是无缓存的 chennels，故此如果是阻塞等待的话，会憋死在第一个写入操作
	go func() {
		wg.Wait()
		for _, ch := range c {
			close(ch)
		}
		for _, ch := range l {
			close(ch)
		}
	}()

	// 计算语料和各个类型下的总词数，单 goroutine 遍历各个 c channels
	corpus_length := 0
	category_length := make(map[string]int)
	// waitgroup 等待 numWorkers + 1 个任务，一个读 c，numWorkers 个读 l 中对应的 channel
	wg_recv.Add(numWorkers + 1)
	// 单 goroutine 中读取 c，由于单 goroutine，必然不会 race condition，故此不需要对外部变量加锁
	// 这个步骤相对 trivial 故此没有按桶分别处理
	go func(c <-chan catecount) {
		for _, ch := range c {
			for cc := range ch {
				corpus_length += cc.count
				category_length[cc.cate] += cc.count
			}
		}
		wg_recv.Done()
	}(c)

	// numWorkers 个 goroutine ，每个从 l 的一个 channel 中处理数据
	// 由于 l 中的 channels 是按桶分开的数据，故此，各 channel 中的类型不会冲突，故此可以最终得到各个类型下每个词的数量
	// 另外，还可以统计该桶下的各个词的数量
	// 得到的结果再分别扔到两个 channels 中去，其会在后面汇总结果
	ch_term := make(chan map[string]int)
	ch_cate := make(chan map[string](map[string]int))
	for i := 0; i < numWorkers; i++ {
		go func(ch <-chan catedetail) {
			defer wg_recv.Done()
			category_term_count := make(map[string](map[string]int))
			term_count := make(map[string]int)
			for cd := range ch {
				for t, c := range cd.detail {
					term_count[t] += c
					_, ok := category_term_count[cd.cate]
					if !ok {
						category_term_count[cd.cate] = make(map[string]int)
					}
					category_term_count[cd.cate][t] += c
				}
			}
			select {
			case ch_term <- term_count:
			case <-done:
				return
			}
			select {
			case ch_cate <- category_term_count:
			case <-done:
				return
			}
		}(l[uint32(i)])
	}

	// 继续异步等待，等到之后说明所有 c/l channels 都已经处理完毕，那么关闭 ch_term and ch_cate ，不会再进数据了
	go func() {
		wg_recv.Wait()
		fmt.Println("TotalLength", corpus_length)
		close(ch_cate)
		close(ch_term)
	}()

	// 定义外部变量
	category_term_count := make(map[string](map[string]int))
	term_count := make(map[string]int)
	var wg_merge sync.WaitGroup
	wg_merge.Add(2)
	go func() {
		for tc := range ch_term {
			for t, c := range tc {
				term_count[t] += c
			}
		}
		wg_merge.Done()
	}()
	go func() {
		for ctc := range ch_cate {
			for cate, detail := range ctc {
				category_term_count[cate] = detail
			}
		}
		wg_merge.Done()
	}()
	wg_merge.Wait()
	// 同步等待，这里得到最终结果 words
	if err := <-errc; err != nil {
		fmt.Println("Err:", err)
	}
	return words, corpus_length
}

func chisquare_modified(cnt_term_category float32, cnt_category float32, cnt_term float32, cnt_whole float32) {
	/*
	   cnt_term_category: 在 category 中 term 的出现次数
	   cnt_category: 在 category 中出现的所有 terms 的总数
	   cnt_term: term 在所有 categories 中出现的总数
	   cnt_whole: 所有 categories 中出现的所有 terms 的总数
	*/
	E1 := cnt_term / cnt_whole * cnt_category
	E2 := cnt_term / cnt_whole * (cnt_whole - cnt_category)
	T1 := cnt_term_category * math.log(cnt_term_category/E1)
	cnt_other := cnt_term - cnt_term_category
	T2 = cnt_other * math.log(cnt_other/E2)
	LL := 2.0 * (T1 + T2)
	return LL
}

func main() {
	numWorkers := 20
	numCores := 20
	maxlength := 5
	runtime.GOMAXPROCS(numCores)
	start := time.Now().UnixNano()
	words, corpus_length := CountFiles(maxlength, "./", numWorkers)
	counting_done := time.Now().UnixNano()
	fmt.Println("[Counting]:", TimeCost(start, counting_done))
	statEntropy(words, numWorkers)
	stat_done := time.Now().UnixNano()
	fmt.Println("[Entropy]:", TimeCost(counting_done, stat_done))
	score(words, corpus_length, numWorkers)
	score_done := time.Now().UnixNano()
	fmt.Println("[Score]:", TimeCost(stat_done, score_done))
	dumpFile("candidates_statistics.go.csv", words)
	fmt.Println("[Dump]:", TimeCost(score_done, time.Now().UnixNano()))
}
