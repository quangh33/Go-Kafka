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

	command := os.Args[1]

	switch command {
	case "produce":
		handleProduce(brokerAddr)
	case "consume":
		handleConsume(brokerAddr)
	default:
		log.Fatalf("unknown command: %s", command)
	}
}

func handleProduce(initialBrokerAddr string) {
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

	currentBrokerAddr := initialBrokerAddr

	for i := 0; i < 5; i++ { // Retry up to 5 times
		log.Printf("Attempting to produce to broker at %s", currentBrokerAddr)
		conn, err := grpc.NewClient(currentBrokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		defer conn.Close()

		client := api.NewKafkaClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		req := &api.ProduceRequest{
			Topic:     topic,
			Partition: uint32(partition),
			Value:     []byte(message),
		}

		resp, err := client.Produce(ctx, req)
		if err != nil {
			log.Fatalf("could not produce: %v", err)
		}

		if resp.ErrorCode == api.ErrorCode_NONE {
			log.Printf("Message produced successfully (offset from leader is approximate)")
			return
		}

		if resp.ErrorCode == api.ErrorCode_NOT_LEADER {
			log.Printf("Not the leader, leader is at %s. Retrying...", resp.LeaderAddr)
			currentBrokerAddr = resp.LeaderAddr
			time.Sleep(1 * time.Second)
			continue
		}
	}
	log.Fatal("Failed to produce message after multiple retries")
}

func handleConsume(brokerAddr string) {
	if len(os.Args) != 5 {
		printUsage()
		log.Fatal("consume command requires group_id, topic, and partition")
	}
	// Consume logic does not need leader redirection for this implementation
	// as any node can serve reads.
	conn, err := grpc.NewClient(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := api.NewKafkaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	groupID := os.Args[2]
	topic := os.Args[3]
	partition, err := strconv.ParseUint(os.Args[4], 10, 32)
	if err != nil {
		log.Fatalf("invalid partition: %v", err)
	}

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
