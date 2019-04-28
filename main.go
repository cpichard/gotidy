package main

import "fmt"
import "log"

import "os"
import "io"
import "path"
import "io/ioutil"

import "crypto/sha256"
import "sync"
import "flag"

//import "strings"

// Number of dirs read in parallel
var concurrentScanQueue = make(chan struct{}, 20)

// Number of file hashing done in parallel
var concurrentProcessQueue = make(chan struct{}, 4)

// Use a work group to know when all dirs are processed
var wgScanDir sync.WaitGroup
var wgProcessFile sync.WaitGroup

// Key Type used for hash key
type Key [32]byte

// The map used to store (hash:[filename])
var shaes2 sync.Map

func hashFile(filePath string) {
	defer wgProcessFile.Done()
	defer func() { <-concurrentProcessQueue }()
	// Will lock if the queue is full
	concurrentProcessQueue <- struct{}{}

	//fmt.Printf("Opening %s\n", filePath)
	f, errOpen := os.Open(filePath)
	if errOpen != nil {
		log.Fatal(errOpen)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}
	sum := h.Sum(nil)
	var key Key
	copy(key[:], sum)
	actual, loaded := shaes2.LoadOrStore(key, []string{filePath})
	if loaded {
		shaes2.Store(key, append(actual.([]string), filePath))
	}
}

func scanDirs(rootDir string) {
	defer wgScanDir.Done()
	defer func() { <-concurrentScanQueue }()

	// Lock if the queue is full
	concurrentScanQueue <- struct{}{}

	files, err := ioutil.ReadDir(rootDir)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		filePath := path.Join(rootDir, file.Name())
		if file.Mode().IsRegular() {
			wgProcessFile.Add(1)
			go hashFile(filePath)
		} else if file.Mode().IsDir() {
			wgScanDir.Add(1)
			go scanDirs(filePath)
		}
	}
}

func printKeyValue(key, values interface{}) bool {
	found := values.([]string)
	if len(found) > 1 {
		fmt.Printf("key:%x\n", key)
		for _, v := range found {
			fmt.Printf("    %s\n", v)
		}
	}
	return true
}

func main() {
	// Look for all files
	var rootDir string
	flag.StringVar(&rootDir, "dir", "", "root dir of the scan")
	flag.Parse()
	if rootDir == "" {
		cwDir, err := os.Getwd()
		if err != nil {
			fmt.Println(err)
		}
		rootDir = cwDir
	}
	fmt.Println(rootDir)
	wgScanDir.Add(1)
	go scanDirs(rootDir)

	wgScanDir.Wait()
	fmt.Printf("All directories parsed\n")
	wgProcessFile.Wait()
	// iterate over key values
	shaes2.Range(printKeyValue)
}
