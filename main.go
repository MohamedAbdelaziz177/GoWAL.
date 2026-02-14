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

	fmt.Println("new Size after delete:", kv.Size())

	fmt.Println("Checkpointing ...")
	if err := kv.Checkpoint("data/snapshot.db"); err != nil {
		log.Fatal(err)
	}

	/*
		if err := kv.CheckPointWithoutTruncating("data/snapshot.db"); err != nil {
			log.Fatal(err)

		}
	*/

	fmt.Println("Checkpoint created. WAL truncated.")

	if err := kv.Set("key16", "value16"); err != nil {
		log.Fatalf("Failed to set key16: %v", err)
	}
	if err := kv.Delete("key1"); err != nil {
		log.Fatalf("Failed to delete key1: %v", err)
	}

	val, _ := kv.Get("key16")
	fmt.Printf("Current State before crash: key16 = %s, Total Size: %d\n", val, kv.Size())

	/* اعتبؤها كراش مثلا */
	if err := kv.Close(); err != nil {
		log.Fatal(err)
	}

	/* here, it recovers after the crash  */
	newKv, err := NewKVStore("data/demo.wal")

	if err != nil {
		log.Fatalf("Failed to recover: %v", err)
	}

	defer newKv.Close()

	val16, exists16 := newKv.Get("key16")
	val1, exists1 := newKv.Get("key1")

	fmt.Println("Results after WAL recovery:")

	fmt.Printf("key16 exists: %v, value: %s\n", exists16, val16)

	fmt.Printf("key1 exists? : %v, value: %s\n", exists1, val1)
}

func cleanPrevFiles() {

	os.MkdirAll("data", 0755)
	os.Remove("data/demo.wal")
	os.Remove("data/snapshot.db")
}
