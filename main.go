package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"golang.org/x/sync/syncmap"
)

//STRUCTS
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

type Stopwatch struct {
	start, stop time.Time
}

func check(err error) {
	if err != nil {
		fmt.Errorf(err.Error())
	}
}

func main() {

	//SETUP
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

	//PROCESS TEXT FILES
	searchDir := "text-files/"
	var fileList []string
	err := filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		fileList = append(fileList, path)
		return nil
	})
	check(err)
	for _, file := range fileList {
		if strings.HasSuffix(file, ".txt") {
			channelWaitGroup.Add(1)
			go processFile(file, lineChan, &channelWaitGroup)
		}
	}
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

	// COLLECT DATA -> WRITE TO RESULTS FILE

	d := lineDupMap.NumDuplicates()
	lm, err := lineMedian.returnMedian()
	check(err)
	ls, err := lineStdDev.returnStdDev()
	check(err)
	tm, err := tokenMedian.returnMedian()
	check(err)
	ts, err := tokenStdDev.returnStdDev()
	check(err)
	keywords := keyWordStorage.ReturnKeywords()

	dString := strconv.Itoa(d)
	lmString := strconv.FormatFloat(float64(lm), 'f', -1, 64)
	lsString := strconv.FormatFloat(ls, 'f', -1, 64)
	tmString := strconv.FormatFloat(float64(tm), 'f', -1, 64)
	tsString := strconv.FormatFloat(ts, 'f', -1, 64)
	var keyWordKeys string
	for _, key := range keywords {
		keyWordKeys += key[0] + "\t"
	}
	var keyWordFrequences string
	for _, freq := range keywords {
		keyWordFrequences += freq[1] + "\t"
	}

	resultsFile, err := os.Create("results.tsv")
	check(err)

	w := new(tabwriter.Writer)
	w.Init(resultsFile, 0, 4, 0, '\t', 0)
	fmt.Fprintln(w, "d\tlm\tls\ttm\tts\t"+keyWordKeys)
	fmt.Fprintln(w, dString+"\t"+lmString+"\t"+lsString+"\t"+tmString+"\t"+tsString+"\t"+keyWordFrequences)
	w.Flush()
	resultsFile.Close()
}

//KEYWORD FUNCTIONALITY CODE

//InitializeKeywords initializes a map with the keywords from a specified file
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
	}
}

//CHeckForKeywords is a streaming function that uses a syncmap to check if a word is a keyword O(1)
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

//ReturnKeywords returns all the keywords and frequencies - O(N) where N is the number of keyword results (lower bound)
func (ks *keyWordStorage) ReturnKeywords() [][]string {
	var returnArray [][]string
	ks.Keywords.Range(func(k, v interface{}) bool {
		returnArray = append(returnArray, []string{k.(string), strconv.Itoa(v.(int))})
		return true
	})
	return returnArray
}

//CheckForDuplicate takes in any given line, and checks if it has been seen before - O(1)
func (ldm *lineDuplicateMap) CheckForDuplicate(line string, wg *sync.WaitGroup) {
	defer wg.Done()
	_, exists := ldm.Lines.LoadOrStore(line, 1)
	if exists {
		ldm.numDupes++
	}
}

//NumDuplicates returns the number of duplicate lines seen over time in O(1)
func (ldm *lineDuplicateMap) NumDuplicates() int {
	return ldm.numDupes
}

//AddToMedian takes any given value and adds it to the Map, so that median can be calculated quickly in the future - O(1)
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

//returnMedian returns the median of values seen so far O(N), where N < 4000
func (ms *medianStorageArray) returnMedian() (float32, error) {
	start := time.Now()
	fmt.Printf("Calculating Median took: %v\n", time.Since(start))

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

//nextBucket abstracts out logic from the returnMedian function, finding the next non-zero bucket to compute avg of two values
func nextBucket(index int, array [4000]int) (int, error) {
	index += 1
	for index < len(array) {
		if array[index] > 0 {
			return index, nil
		}
		index++
	}
	return -1, errors.New("reached end of array")
}

//AddValue is a O(1) streaming algorithm that adds a value to a running approx of variance
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

//returnStdDev is a O(1) algorithm that returns the standard deviation based on a set of numbers seen thus far
func (svc *stdVarianceCalculator) returnStdDev() (float64, error) {
	start := time.Now()
	fmt.Printf("Calculating StdDev took: %v\n", time.Since(start))

	svc.mutex.RLock()
	defer svc.mutex.RUnlock()

	if svc.iterations < 2 {
		return -1, errors.New("Empty List")
	} else {
		variance := float64(svc.runningM2) / float64(svc.iterations-1)
		return math.Sqrt(variance), nil
	}
}

//processFile reads through a file, sending its contents to a channel, which is used to process data
func processFile(filename string, lineChan chan string, channelWaitGroup *sync.WaitGroup) {
	defer channelWaitGroup.Done()

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	start := time.Now()
	for scanner.Scan() {
		line := scanner.Text()
		lineChan <- line
	}
	elapsedTime := time.Since(start)
	fmt.Printf("Reading in "+filename+" took: %v\n", elapsedTime)
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
