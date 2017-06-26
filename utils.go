package main

import (
	"math"
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

func ChisquareModified(cnt_term_category float64, cnt_category float64, cnt_term float64, cnt_whole float64) float64 {
	/*
	   cnt_term_category: 在 category 中 term 的出现次数
	   cnt_category: 在 category 中出现的所有 terms 的总数
	   cnt_term: term 在所有 categories 中出现的总数
	   cnt_whole: 所有 categories 中出现的所有 terms 的总数
	*/
	E1 := cnt_term / cnt_whole * cnt_category
	E2 := cnt_term / cnt_whole * (cnt_whole - cnt_category)
	T1 := cnt_term_category * math.Log(cnt_term_category/E1)
	cnt_other := cnt_term - cnt_term_category
	T2 := cnt_other * math.Log(cnt_other/E2)
	LL := 2.0 * (T1 + T2)
	return LL
}
