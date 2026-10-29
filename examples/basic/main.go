package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"time"

	smkv "github.com/m6yf/smkv-go"
)

func main() {
	address := flag.String("addr", "127.0.0.1:7379", "SMKV node address")
	flag.Parse()
	client, err := smkv.NewClient(*address, smkv.Options{PoolSize: 4})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := []byte("go-sdk/example")
	if err := client.Put(ctx, key, []byte("hello from Go"), smkv.WithTTL(time.Minute)); err != nil {
		log.Fatal(err)
	}
	value, err := client.Get(ctx, key)
	if errors.Is(err, smkv.ErrNotFound) {
		fmt.Println("cache miss")
		return
	}
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(value))
	if err := client.Delete(ctx, key); err != nil {
		log.Fatal(err)
	}
}
