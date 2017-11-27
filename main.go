package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"golang.org/x/sync/syncmap"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"sync"
)

type medianStorageArray struct {
	mutex          sync.RWMutex
	size           int
	frequencyArray [4000]int
}

type keyWordStorage struct {
	Keywords syncmap.Map
}

type stdVarianceCalculator struct {
	mutex       sync.RWMutex
	runningMean float32
	runningM2   float32
	iterations  float32
}

type lineDuplicateMap struct {
	Lines    syncmap.Map
	numDupes int
}

func NewWriter(w io.Writer) (writer *csv.Writer) {
	writer = csv.NewWriter(w)
	writer.Comma = '\t'
	return writer
}

// O(N), where N is at most 4000
func (ms *medianStorageArray) returnMedian() (float32, error) {

	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	if ms.size == 0 {
		return -1, errors.New("Empty List")
	}

	// If even take an average of two buckets
	even := ((ms.size % 2) == 0)
	desiredPos := ms.size / 2

	i := 0
	index := 0

	if even {
		for i < desiredPos {
			if i <= desiredPos && i+ms.frequencyArray[index] > desiredPos { // in the current bucket
				return float32(index), nil
			} else if i < desiredPos && i+ms.frequencyArray[index] < desiredPos { // not in the current bucket
				i += ms.frequencyArray[index]
				index++
			} else if i < desiredPos && i+ms.frequencyArray[index] == desiredPos {
				nextIndex, err := nextBucket(index, ms.frequencyArray)
				if err != nil {
					return -1, err
				}
				return float32(index+nextIndex) / float32(2), nil
			}
		}
		return -1.0, nil

	} else {

		for i < desiredPos {
			if i <= desiredPos && i+ms.frequencyArray[index] > desiredPos { // in the current bucket
				return float32(index), nil
			} else if i < desiredPos && i+ms.frequencyArray[index] <= desiredPos { // not in the current bucket
				i += ms.frequencyArray[index]
				index++
			}
		}
		return -1.0, nil
	}
}

func nextBucket(index int, array [4000]int) (int, error) {
	index += 1
	for index < len(array) {
		if array[index] > 0 {
			return index, nil
		}
	}
	return -1, errors.New("reached end of array")
}

func (svc *stdVarianceCalculator) returnVariance() (float32, error) {
	svc.mutex.RLock()
	defer svc.mutex.RUnlock()

	if svc.iterations < 2 {
		return -1, errors.New("Empty List")
	} else {
		return float32(svc.runningM2) / float32(svc.iterations-1), nil
	}
}

func (svc *stdVarianceCalculator) AddValue(x int, wg *sync.WaitGroup) {
	svc.mutex.Lock()
	defer svc.mutex.Unlock()
	defer wg.Done()

	svc.iterations += 1
	delta := float32(x) - svc.runningMean
	svc.runningMean += (delta / svc.iterations)
	delta2 := float32(x) - svc.runningMean
	svc.runningM2 += delta * delta2
}

func (ks *keyWordStorage) InitializeKeywords(filename string) {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.ToLower(line)
		ks.Keywords.Store(line, 0)
		fmt.Println(line)
	}
}

func check(err error) {
	if err != nil {
		fmt.Errorf(err.Error())
	}
}

func main() {

	waitGroup := sync.WaitGroup{}
	channelWaitGroup := sync.WaitGroup{}

	lineChan := make(chan string)
	keyWordStorage := keyWordStorage{}
	tokenMedian := medianStorageArray{}
	lineMedian := medianStorageArray{}
	tokenStdDev := stdVarianceCalculator{}
	lineStdDev := stdVarianceCalculator{}
	lineDupMap := lineDuplicateMap{}

	keyWordStorage.InitializeKeywords("keywords.txt")
	channelWaitGroup.Add(2)
	go processFile("sample.txt", lineChan, &channelWaitGroup)
	go processFile("text2.txt", lineChan, &channelWaitGroup)
	go func() {
		channelWaitGroup.Wait()
		close(lineChan)
	}()
	for line := range lineChan {
		lineLen := len(line)
		waitGroup.Add(3)
		go lineDupMap.CheckForDuplicate(line, &waitGroup)
		go lineStdDev.AddValue(lineLen, &waitGroup)
		go lineMedian.AddToMedian(lineLen, &waitGroup)

		for _, token := range strings.Fields(line) {
			tokenLen := len(token)
			waitGroup.Add(3)
			go tokenStdDev.AddValue(tokenLen, &waitGroup)
			go tokenMedian.AddToMedian(tokenLen, &waitGroup)
			go keyWordStorage.CheckForKeywords(token, &waitGroup)
		}
	}

	waitGroup.Wait()
	keyWordStorage.PrintKeywords()
	ts, err := tokenStdDev.returnVariance()
	fmt.Println(math.Sqrt(float64(ts)))
	check(err)
	dupes := lineDupMap.NumDuplicates()
	fmt.Println(dupes)
	tm, err := tokenMedian.returnMedian()
	check(err)
	fmt.Println(tm)
	results, err := os.Create("results.tsv")
	check(err)
	defer results.Close()
	writer := NewWriter(results)
	writer.Write([]string{"one", "two", "five"})
	writer.Write([]string{"three", "four"})
	writer.Write([]string{"six", "seven"})
	writer.Flush()
}

func (ks *keyWordStorage) CheckForKeywords(word string, wg *sync.WaitGroup) {
	defer wg.Done()
	word = strings.ToLower(word)
	val, ok1 := ks.Keywords.Load(word)
	if ok1 {
		num, ok2 := val.(int)
		if ok2 {
			ks.Keywords.Store(word, num+1)
		}
	}
}

func (ks *keyWordStorage) PrintKeywords() {
	ks.Keywords.Range(func(k, v interface{}) bool {
		fmt.Print(k.(string) + ": ")
		fmt.Println(v.(int))
		return true
	})
}

func (ldm *lineDuplicateMap) NumDuplicates() int {
	return ldm.numDupes
}

func (ldm *lineDuplicateMap) CheckForDuplicate(line string, wg *sync.WaitGroup) {
	defer wg.Done()
	_, exists := ldm.Lines.LoadOrStore(line, 1)
	if exists {
		ldm.numDupes++
	}
}

func (ms *medianStorageArray) AddToMedian(num int, wg *sync.WaitGroup) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()
	defer wg.Done()

	if num >= 4000 {
		return errors.New("index too large, line is too long")
	}
	ms.frequencyArray[num] = ms.frequencyArray[num] + 1
	ms.size++
	return nil
}

func processFile(filename string, lineChan chan string, channelWaitGroup *sync.WaitGroup) {
	defer channelWaitGroup.Done()

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		lineChan <- line
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
