package main

import (
	"Go-Kafka/api"
	"context"
	"flag"
	"fmt"
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
		return
	}

	produceCmd := flag.NewFlagSet("produce", flag.ExitOnError)
	produceAcks := produceCmd.String("acks", "all", "Acknowledgment level (none, all)")

	consumeCmd := flag.NewFlagSet("consume", flag.ExitOnError)

	switch os.Args[1] {
	case "produce":
		produceCmd.Parse(os.Args[2:])
		if produceCmd.NArg() != 4 {
			fmt.Println("Usage: client produce [--acks=<level>] <broker_addr> <topic> <partition> <value>")
			return
		}
		brokerAddr := produceCmd.Arg(0)
		topic := produceCmd.Arg(1)
		partition, err := strconv.ParseUint(produceCmd.Arg(2), 10, 32)
		if err != nil {
			log.Fatalf("Invalid partition: %v", err)
		}
		value := produceCmd.Arg(3)
		handleProduce(brokerAddr, topic, uint32(partition), value, *produceAcks)
	case "consume":
		consumeCmd.Parse(os.Args[2:])
		if consumeCmd.NArg() != 4 {
			fmt.Println("Usage: client consume <broker_addr> <group_id> <topic> <partition>")
			return
		}
		brokerAddr := consumeCmd.Arg(0)
		groupID := consumeCmd.Arg(1)
		topic := consumeCmd.Arg(2)
		partition, err := strconv.ParseUint(consumeCmd.Arg(3), 10, 32)
		if err != nil {
			log.Fatalf("Invalid partition: %v", err)
		}
		handleConsume(brokerAddr, groupID, topic, uint32(partition))
	default:
		printUsage()
	}
}

func printUsage() {
	fmt.Println("Usage: client <command> [args]")
	fmt.Println("Commands:")
	fmt.Println("  produce [--acks=<level>] <broker_addr> <topic> <partition> <value>")
	fmt.Println("  consume <broker_addr> <group_id> <topic> <partition>")
}

func handleProduce(initialBrokerAddr, topic string, partition uint32, value string, acks string) {
	var acksLevel api.AckLevel
	switch strings.ToLower(acks) {
	case "none":
		acksLevel = api.AckLevel_NONE
	case "all":
		acksLevel = api.AckLevel_ALL
	default:
		log.Fatalf("Invalid acks level: %s. Must be 'none' or 'all'", acks)
	}

	currentBrokerAddr := initialBrokerAddr
	for i := 0; i < 5; i++ { // Retry up to 5 times for redirection
		log.Printf("Attempting to produce to broker at %s", currentBrokerAddr)
		conn, err := grpc.NewClient(currentBrokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		defer conn.Close()

		c := api.NewKafkaClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		req := &api.ProduceRequest{
			Topic:     topic,
			Partition: partition,
			Value:     []byte(value),
			Ack:       acksLevel,
		}

		resp, err := c.Produce(ctx, req)
		if err != nil {
			log.Printf("could not produce: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if resp.ErrorCode == api.ErrorCode_NOT_LEADER {
			log.Printf("Not the leader, leader is at %s. Retrying...", resp.LeaderAddr)
			currentBrokerAddr = resp.LeaderAddr
			time.Sleep(1 * time.Second)
			continue
		}

		if acksLevel == api.AckLevel_ALL {
			log.Printf("Message produced successfully to offset: %d", resp.Offset)
		} else {
			log.Println("Message sent successfully (fire-and-forget)")
		}
		return
	}
	log.Fatalf("Failed to produce message after multiple retries")
}

func handleConsume(brokerAddr, groupID string, topic string, partition uint32) {
	conn, err := grpc.NewClient(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := api.NewKafkaClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	fetchReq := &api.FetchOffsetRequest{
		ConsumerGroupId: groupID,
		Topic:           topic,
		Partition:       partition,
	}
	fetchResp, err := c.FetchOffset(ctx, fetchReq)
	if err != nil {
		log.Fatalf("could not fetch offset: %v", err)
	}
	currentOffset := fetchResp.Offset
	log.Printf("Starting consumption for group '%s' from offset %d", groupID, currentOffset)

	for {
		consumeCtx, consumeCancel := context.WithTimeout(context.Background(), time.Second*10)
		defer consumeCancel()

		consumeReq := &api.ConsumeRequest{
			Topic:     topic,
			Partition: partition,
			Offset:    currentOffset,
		}
		consumeResp, err := c.Consume(consumeCtx, consumeReq)
		if err != nil {
			if strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "offset out of bounds") {
				log.Println("Reached end of the log.")
				break
			}
			log.Fatalf("could not consume: %v", err)
		}

		log.Printf("Consumed message: '%s' (next offset: %d)", string(consumeResp.Record.Value), consumeResp.Record.Offset)
		currentOffset = consumeResp.Record.Offset

		commitCtx, commitCancel := context.WithTimeout(context.Background(), time.Second*10)
		defer commitCancel()
		commitReq := &api.CommitOffsetRequest{
			ConsumerGroupId: groupID,
			Topic:           topic,
			Partition:       partition,
			Offset:          currentOffset,
		}
		if _, err := c.CommitOffset(commitCtx, commitReq); err != nil {
			log.Fatalf("could not commit offset: %v", err)
		}
	}
}
