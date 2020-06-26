//
package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
)

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

func scanOneDirectory(rootDir string) {
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
			go scanOneDirectory(filePath)
		}
	}
}

func printMultipleValues(key, values interface{}) bool {
	found := values.([]string)
	if len(found) > 1 {
		fmt.Printf("key:%x\n", key)
		for _, v := range found {
			fmt.Printf("    %s\n", v)
		}
	}
	return true
}

func showDeletable(rootDir string, key, values interface{}) bool {
	found := values.([]string)
	if len(found) > 1 {
		var isInRoot = false
		for _, v := range found {
			// Search for entry in the rootDir

			if strings.HasPrefix(v, rootDir) {
				isInRoot = true
				break
			}
		}

		if isInRoot == true {
			for _, v := range found {
				// Search for entry in the rootDir
				if !strings.HasPrefix(v, rootDir) {
					fmt.Printf("%s\n", v)
					// os.Remove(v)
				}
			}
		}
	}
	return true
}

func scanDirectories(rootDir string) {
	fmt.Println(rootDir)
	wgScanDir.Add(1)
	go scanOneDirectory(rootDir)

	wgScanDir.Wait()
	wgProcessFile.Wait()
}

var rootDir string
var diffDir string

func init() {
	flag.StringVar(&rootDir, "dir", "", "root dir of the scan")
	flag.StringVar(&diffDir, "compare", "", "dir to compare with")
}

func main() {
	// Look for all files
	flag.Parse()
	if rootDir == "" {
		cwDir, err := os.Getwd()
		if err != nil {
			fmt.Println(err)
		}
		rootDir = cwDir
	}
	scanDirectories(rootDir)

	if diffDir != "" {
		scanDirectories(diffDir)
		shaes2.Range(func(key, values interface{}) bool { return showDeletable(rootDir, key, values) })
	} else {
		// iterate over key values
		shaes2.Range(printMultipleValues)
	}

}
