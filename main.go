package main

import (
	"context"
	"fmt"
	amqp "github.com/Azure/go-amqp"
	"log"
	"time"
)

func main() {
	// create connection
	conn, err := amqp.Dial("amqp://localhost:5672", &amqp.ConnOptions{
		SASLType: amqp.SASLTypePlain("admin", "admin"),
	})
	if err != nil {
		log.Fatal("Dialing AMQP server:", err)
	}
	defer conn.Close()

	ctx := context.TODO()

	// open a session
	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		log.Fatal("Creating AMQP session:", err)
	}

	// send a message
	{
		// create a sender
		sender, err := session.NewSender(ctx, "/ard-issue-instruction", nil)
		if err != nil {
			log.Fatal("Creating sender link:", err)
		}

		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)

		// send message
		err = sender.Send(ctx, amqp.NewMessage([]byte("Hello!")))
		if err != nil {
			log.Fatal("Sending message:", err)
		}

		sender.Close(ctx)
		cancel()
	}

	// continuously read messages
	{
		// create a receiver
		receiver, err := session.NewReceiver(ctx, "/ard-issue-instruction", &amqp.ReceiverOptions{
			Credit: 10,
		})
		if err != nil {
			log.Fatal("Creating receiver link:", err)
		}
		defer func() {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			receiver.Close(ctx)
			cancel()
		}()

		for {
			// receive next message
			msg, err := receiver.Receive(ctx)
			if err != nil {
				log.Fatal("Reading message from AMQP:", err)
			}

			// accept message
			if err = receiver.AcceptMessage(context.TODO(), msg); err != nil {
				log.Fatalf("Failure accepting message: %v", err)
			}

			fmt.Printf("Message received: %s\n", msg.GetData())
		}
	}
}
