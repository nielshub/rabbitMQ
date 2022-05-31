package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	amqp "github.com/streadway/amqp"
)

var notifyClose = make(chan *amqp.Error)

func MaxParallelism() int {
	maxProcs := runtime.GOMAXPROCS(0)
	numCPU := runtime.NumCPU()
	if maxProcs < numCPU {
		return maxProcs
	}
	return numCPU
}

// newConsumer is a function create a rabbitMQ consumer
func newConsumer(connectionString string, fetchCount int) (connection *amqp.Connection, channel *amqp.Channel) {
	connection, _ = amqp.Dial(connectionString)
	channel, _ = connection.Channel()
	channel.Qos(fetchCount, 0, false)

	channel.NotifyClose(notifyClose)
	channel.NotifyPublish(notifyConfirm)
	return
}

func worker(channel *amqp.Channel, done chan bool, queueName, consumerName string) {
	msgs, _ := channel.Consume(
		queueName,    // queue
		consumerName, // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)

	for m := range msgs {
		body := string(m.Body)
		log.Printf("Processing data %+v\n", body)
		log.Printf("Processing data %+v done\n", body)
		time.Sleep(5 * time.Second)
		m.Ack(false)
		log.Printf("Data %+v acked\n", body)
	}
	done <- true
}

func main() {
	defer log.Println("Program stopped successful")
	url := "amqp://user:password@localhost:5672/"
	queue := "my_queue"
	name := "consumer"
	fetchSize := 10

	maxWorkers := MaxParallelism()
	fmt.Println("Maximum workers:", maxWorkers)
	log.Printf("Connecting to %s queue %s fetch-size %d\n", url, queue, fetchSize)
	connection, channel := newConsumer(url, fetchSize)
	log.Printf("Consumer %s is subscribing queue %s\n", name, queue)

	defer connection.Close()
	defer channel.Close()
	defer log.Println("Closing qeueu channel and connection")

	done := make(chan bool, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		go worker(channel, done, queue, "consumer-"+strconv.Itoa(i))
	}

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-notifyClose:
		closeWorkes(channel)
	case <-exit:
		closeWorkes(channel)
		log.Println("Got exit signal")
		// Stop recieving message from queue
		log.Println("Stopped receiving message from queue")
		// Wait for worker procrss recieved message
		log.Println("Wait for worker procrss recieved message")
	}
	<-done
	log.Println("Woker done")
	// Just to print if all goroutines are closed or not
	// it is returning 4 and that is correct
	// the main goroutine
	// the background sweeper (the phase of the garbage collection which is concurrent)
	// the scavenger (also part of the garbage collector)
	// the finalizer goroutine (exclusively running the finalizers eventually attached to objects)
	time.Sleep(2 * time.Second)
	fmt.Println("Remaining goroutines", runtime.NumGoroutine())
}

func closeWorkes(channel *amqp.Channel) {
	maxWorkers := MaxParallelism()
	for i := 0; i < maxWorkers; i++ {
		channel.Cancel("consumer-"+strconv.Itoa(i), false)
	}
}
