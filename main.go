package main

import (
	"context"
	"fmt"
	"log"
)

func main() {
	ctx := context.TODO()

	conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/", amqp.ConnSASLPlain("admin", "admin"))
	if err != nil {
		log.Fatalf("Failed to connect to ActiveMQ: %v", err)
	}
	defer conn.Close()

	session, err := conn.NewSession()
	if err != nil {
		log.Fatalf("Failed to create a session: %v", err)
	}
	defer session.Close(ctx)

	receiver, err := session.NewReceiver(
		amqp.LinkSourceAddress("test-queue"),
		amqp.LinkCredit(10),
	)
	if err != nil {
		log.Fatalf("Failed to create a receiver: %v", err)
	}
	defer receiver.Close(ctx)

	for i := 0; i < 10; i++ {
		msg, err := receiver.Receive(ctx)
		if err != nil {
			log.Fatalf("Failed to receive a message: %v", err)
		}

		fmt.Println("Received message:", string(msg.GetData()))
	}
}
