package main

import (
	// "fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"unicode"
)

func IsChineseWide(r rune) bool {
	return unicode.Is(unicode.Scripts["Han"], r)
}

var NarrowHan = &unicode.RangeTable{
	R16: []unicode.Range16{
		{0x4e00, 0x9fcb, 1},
	},
}

func IsChinese(r rune) bool {
	return unicode.Is(NarrowHan, r)
}

func FilterChineseRough(str string, sub string) string {
	reg := regexp.MustCompile("[\\s\\d,.<>/?:;'\"[\\]{}()\\|~!@#$%^&*\\-_=+a-zA-Z，。《》、？：；“”‘’｛｝【】（）…￥！—┄－]+")
	return reg.ReplaceAllString(str, sub)
}

func FilterChinese(str string) []rune {
	old := []rune(str)
	length := len(old)
	runes := make([]rune, 0, length)
	for _, c := range old {
		if IsChinese(c) {
			runes = append(runes, c)
		}
	}
	return runes
}

func Ulen(s string) int {
	return len([]rune(s))
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func TimeCost(start int64, end int64) float64 {
	return float64(end-start) / float64(1000000*1000)
}

func MergeWord(words map[string]*Word, w *Word) {
	var text string = w.text
	_, ok := words[text]
	if !ok {
		words[text] = w
	} else {
		words[text].freq += w.freq
		for r, c := range w.left {
			words[text].left[r] += c
		}
		for r, c := range w.right {
			words[text].right[r] += c
		}
	}
}

func MergeMap(words map[string]*Word, source map[string]*Word) {
	for _, w := range source {
		MergeWord(words, w)
	}
	source = make(map[string]*Word)
}

func MergeMapNoConflict(words map[string]*Word, source map[string]*Word) {
	for text, w := range source {
		words[text] = w
	}
	source = make(map[string]*Word)
}

func CheckError(e error) {
	if e != nil {
		panic(e)
	}
}

func WriteStringsToFile(fname string, contents []string, perm os.FileMode) {
	ioutil.WriteFile(fname, []byte(strings.Join(contents, "\n")), perm)
}

func LoadFileToStrings(fname string) ([]string, error) {
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		return []string{}, err
	} else {
		strdata := string(data)
		strdata = strings.Replace(strdata, "\r\n", "\n", -1)
		return strings.Split(strdata, "\n"), nil
	}
}

func GetBucket(s string, bucket uint32) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32() % bucket
}
