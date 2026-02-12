package main

import (
	"fmt"
	"log"
	"os"
)

func main() {

	cleanPrevFiles()

	kv, err := NewKVStore("data/demo.wal")

	if err != nil {
		log.Fatal(err)
	}

	for i := 1; i <= 15; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)

		if err := kv.Set(key, value); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Size after delete:", kv.Size())

	fmt.Println("Creating checkpoint...")

	if err := kv.CheckPointWithoutTruncating("data/snapshot.db"); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Checkpoint created. WAL truncated.")

	if err := kv.Close(); err != nil {
		log.Fatal(err)
	}

}

func cleanPrevFiles() {

	os.MkdirAll("data", 0755)
	os.Remove("data/demo.wal")
	os.Remove("data/snapshot.db")
}
