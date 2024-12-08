package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

type Review struct {
	ReviewID string  `json:"review_id"`
	MovieID  string  `json:"movie_id"`
	UserID   string  `json:"user_id"`
	Title    string  `json:"title"`
	Review   string  `json:"review"`
	Score    float32 `json:"score"`
}

var kafka_config = &kafka.ConfigMap{"bootstrap.servers": "kafka:9092"}
var topic = "ingestion-movie-reviews"
var numPartitions = 4
var replicationFactor = 1

func create_topic(ctx context.Context) {
	adminClient, err := kafka.NewAdminClient(kafka_config)
	if err != nil {
		panic(fmt.Sprintf("Failed to create Admin client: %s\n", err))
	}
	defer adminClient.Close()

	results, err := adminClient.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
		}},
		nil,
	)
	if err != nil {
		fmt.Printf("Failed to create topic: %s\n", err)
	} else {
		for _, result := range results {
			fmt.Printf("Topic %s: %s\n", result.Topic, result.Error.String())
		}
	}
}

func hashTitleToPartition(title string, numPartitions int) int32 {
	h := fnv.New32a()
	h.Write([]byte(title))
	return int32(h.Sum32()) % int32(numPartitions)
}

func makeMovieID(input string) string {
	// Compute the SHA-256 hash of the input string
	hash := sha256.Sum256([]byte(input))

	// Format the first 16 bytes of the hash as a UUID
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		hash[0:4],
		hash[4:6],
		hash[6:8],
		hash[8:10],
		hash[10:16],
	)
}

func sendToQueue(review Review) error {
	p, err := kafka.NewProducer(kafka_config)
	if err != nil {
		return err
	}
	defer p.Close()

	reviewJson, _ := json.Marshal(review)

	// Compute partition by hashing the title
	partition := hashTitleToPartition(review.Title, numPartitions)

	// Partition by movie here
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: partition},
		Value:          reviewJson,
	}, nil)
	p.Flush(1000)

	return nil
}

func ingestReview(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var review Review

		// Decode the message received
		err := json.NewDecoder(r.Body).Decode(&review)
		if err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Assign a new ID
		review.ReviewID = uuid.NewString()
		review.MovieID = makeMovieID(review.Title)

		// Put the message in Queue
		err = sendToQueue(review)
		if err != nil {
			http.Error(w, "Error processing message", http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, "Message received and queued for processing: %v", review)
		log.Printf("Message received and queued for processing: %v", review)
	} else {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
	}
}

func main() {
	ctx := context.Background()

	// Create Kafka topic for ingestion
	create_topic(ctx)

	// Register handlers
	http.HandleFunc("/ingest", ingestReview)

	fmt.Println("Ingestion server running on port 8080...")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}
