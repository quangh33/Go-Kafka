// A simple command-line client to interact with the Go-Kafka broker.

package main

import (
	"Go-Kafka/api"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	brokerAddr := "localhost:9092"
	if os.Getenv("BROKER_ADDR") != "" {
		brokerAddr = os.Getenv("BROKER_ADDR")
	}

	conn, err := grpc.NewClient(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := api.NewKafkaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	command := os.Args[1]

	switch command {
	case "produce":
		handleProduce(ctx, client)
	case "consume":
		handleConsume(ctx, client)
	default:
		log.Fatalf("unknown command: %s", command)
	}
}

func handleProduce(ctx context.Context, client api.KafkaClient) {
	if len(os.Args) != 5 {
		printUsage()
		log.Fatal("produce command requires topic, partition, and message")
	}
	topic := os.Args[2]
	partition, err := strconv.ParseUint(os.Args[3], 10, 32)
	if err != nil {
		log.Fatalf("invalid partition: %v", err)
	}
	message := os.Args[4]

	req := &api.ProduceRequest{
		Topic:     topic,
		Partition: uint32(partition),
		Value:     []byte(message),
	}

	resp, err := client.Produce(ctx, req)
	if err != nil {
		log.Fatalf("could not produce: %v", err)
	}
	log.Printf("Message produced to offset: %d", resp.Offset)
}

func handleConsume(ctx context.Context, client api.KafkaClient) {
	if len(os.Args) != 5 {
		printUsage()
		log.Fatal("consume command requires group_id, topic, and partition")
	}
	groupID := os.Args[2]
	topic := os.Args[3]
	partition, err := strconv.ParseUint(os.Args[4], 10, 32)
	if err != nil {
		log.Fatalf("invalid partition: %v", err)
	}

	// 1. Fetch the last committed offset for this consumer group
	fetchResp, err := client.FetchOffset(ctx, &api.FetchOffsetRequest{
		ConsumerGroupId: groupID,
		Topic:           topic,
		Partition:       uint32(partition),
	})
	if err != nil {
		log.Fatalf("could not fetch offset: %v", err)
	}

	currentOffset := fetchResp.Offset
	log.Printf("Starting consumption for group '%s' from offset %d", groupID, currentOffset)

	// 2. Loop to continuously consume messages
	for {
		consumeReq := &api.ConsumeRequest{
			Topic:     topic,
			Partition: uint32(partition),
			Offset:    currentOffset,
		}

		resp, err := client.Consume(ctx, consumeReq)
		if err != nil {
			if err == io.EOF || strings.Contains(err.Error(), "offset out of bounds") {
				log.Println("Reached end of the log.")
			} else {
				log.Printf("could not consume: %v", err)
			}
			break
		}

		log.Printf("Consumed message: '%s' (next offset: %d)", string(resp.Record.Value), resp.Record.Offset)

		// 3. Commit the new offset
		_, err = client.CommitOffset(ctx, &api.CommitOffsetRequest{
			ConsumerGroupId: groupID,
			Topic:           topic,
			Partition:       uint32(partition),
			Offset:          resp.Record.Offset,
		})
		if err != nil {
			log.Printf("WARNING: could not commit offset: %v", err)
		}

		currentOffset = resp.Record.Offset
	}
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  client produce <topic> <partition> <message>")
	fmt.Println("  client consume <group_id> <topic> <partition>")
}
