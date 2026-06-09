package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"

	redis "github.com/go-redis/redis/v8"
)

const chunkSize = 4 * 1024 // 4KB

type redisClient struct {
	rdb *redis.Client
	ctx context.Context
}

var rc *redisClient //redis client instance

func (rc *redisClient) put(key int, value []byte) error {
	return rc.rdb.Set(rc.ctx, fmt.Sprintf("%d", key), value, 0).Err()
}

func (rc *redisClient) get(key int) ([]byte, error) {
	return rc.rdb.Get(rc.ctx, fmt.Sprintf("%d", key)).Bytes()
}

func daemonHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	//fetching the chunks from the request
	var idx = 0
	for {
		chunk := make([]byte, chunkSize)
		n, err := r.Body.Read(chunk)
		if n > 0 {
			fmt.Printf("Received %d chunk of %d bytes\n", idx, n)
			// Store the chunk in Redis with an incrementing key
			if err := rc.put(idx, chunk[:n]); err != nil {
				log.Printf("Failed to store chunk in Redis: %v", err)
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
			idx++
			log.Printf("Stored chunk %d in Redis\n", idx)
		}
		if err != nil {
			if err.Error() == "EOF" {
				// After all chunks are processed, store the total count
				if err := rc.rdb.Set(rc.ctx, "total_chunks_count", idx, 0).Err(); err != nil {
					log.Printf("Failed to store total chunks count in Redis: %v", err)
					// This is not critical enough to return an error to the client for the upload itself
				} else {
					log.Printf("Stored total chunks count: %d\n", idx)
				}
				break
			}
			log.Printf("Error reading request body: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	}
}
func daemonHandlerReturn(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	query := r.URL.Query()
	keyStr := query.Get("key")
	if keyStr == "" {
		http.Error(w, "Bad Request: missing key parameter", http.StatusBadRequest)
		return
	}

	intKey, err := strconv.Atoi(keyStr)
	if err != nil {
		http.Error(w, "Bad Request: invalid key parameter", http.StatusBadRequest)
		return
	}

	value, err := rc.get(intKey)
	if err != nil {
		if err == redis.Nil {
			http.Error(w, "Chunk not found", http.StatusNotFound)
			return
		}
		log.Printf("Failed to retrieve chunk from Redis: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.Write(value)
}

func totalChunksHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	totalChunksStr, err := rc.rdb.Get(rc.ctx, "total_chunks_count").Result()
	if err == redis.Nil {
		http.Error(w, "No chunks uploaded yet", http.StatusNotFound)
		return
	}
	if err != nil {
		log.Printf("Failed to retrieve total chunks count from Redis: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(totalChunksStr))
}

func main() {
	// Initialize Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     "redis:6379", // Changed from localhost to redis service name
		Password: "pass",       // Use the same password as set in docker-compose.yml
		DB:       0,            // Default DB
	})
	ctx := context.Background()

	rc = &redisClient{rdb: rdb, ctx: ctx}

	http.HandleFunc("/process", daemonHandler)
	http.HandleFunc("/retrieve", daemonHandlerReturn)
	http.HandleFunc("/total-chunks", totalChunksHandler)

	if err := http.ListenAndServe(":8081", nil); err != nil {
		log.Panic("Failed to start daemon server: ", err)
	}
}
