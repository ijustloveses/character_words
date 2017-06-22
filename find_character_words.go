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

type terminfo struct {
	text string
	cate string
	count int
}

type termresult struct {
	text string
	cate string
	count int
	score float32
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

func CountFiles(root string, numWorkers int) (map[string]*Word, int) {
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
	go func(c map[uint32](chan<- catecount)) {
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
				_, ok := category_term_count[cd.cate]
				if !ok {
					category_term_count[cd.cate] = make(map[string]int)
				}
				for t, c := range cd.detail {
					term_count[t] += c
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
		# 由于分桶的，故此这里不需要做比较，每次遍历一定都是新的种类
		for ctc := range ch_cate {
			for cate, detail := range ctc {
				category_term_count[cate] = detail
			}
		}
		wg_merge.Done()
	}()
	// 同步等待，这里 4 个外部变量全部完成统计
	wg_merge.Wait()
	if err := <-errc; err != nil {
		CheckError(err)
	}

	// 到这里还没出错的话，那么继续计算 chi-square
	// 每个种类下出现的词，送入 queue 中
	cterm := make(chan terminfo) 
	go func() {
		for cate, detail := range category_term_count {
			for t, c := range detail {
				cterm <- terminfo{t, cate, c}
			}
		}
	}()

	// numWorkers 个 goroutines 去处理
	// 结果送入结果 channel
	cresult := make(chan termresult)
	var wgfinal WaitGroup
	wgfinal.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wgfinal.Done()
			for term := range cterm {
				if term.count == term_count[term.text] {
					ll := 99999.0
				} else {
					ll := ChisquareModified(float32(term.count), float32(category_length[term.cate]), float32(term_count[term.text]), float32(corpus_length))
				}
				cresult <- termresult{term.t, term.cate, term.count, ll}
			}
		}()
	}
	// 异步等待全部处理结束
	go func() {
		wgfinal.Wait()
		close(cresult)
	}()
	// 整理结果
	final_result := []string{}
	for tr := range termresult {
		final_result = append(final_result, fmt.Sprintf("%s\t%s\t%d\t%f", tr.text, tr.cate, tr.count, tr.score))
	}
	WriteStringsToFile("chi_squares.go.res", final_result, 0644)
}

func main() {
	numWorkers := 20
	numCores := 20
	runtime.GOMAXPROCS(numCores)
	start := time.Now().UnixNano()
	CountFiles("./", numWorkers)
	counting_done := time.Now().UnixNano()
	fmt.Println("[Counting]:", TimeCost(start, counting_done))
}
