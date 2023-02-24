package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	pb "grpc_colleen/protos/longLived"

	"google.golang.org/grpc"
)

func main() {

	lis, err := net.Listen("tcp", "127.0.0.1:7070")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	//creates an empty slice
	grpcServer := grpc.NewServer([]grpc.ServerOption{}...)

	//creates a new variable named server and assigns it a pointer to a new instance of the longlivedServer struct type

	server := &longlivedServer{}

	// Start sending data to subscribers

	go server.mockDataGenerator()

	// When the RegisterLonglivedServer function is called, it associates the server implementation with the gRPC server instance,
	// allowing incoming gRPC requests to be routed to the appropriate methods on the server implementation.

	pb.RegisterLonglivedServer(grpcServer, server)

	log.Printf("Starting server on %s", server)

	//Start listening

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

}

// a "sync.Map," which is a thread-safe map (also known as a dictionary or hash table)
// that can be accessed concurrently by multiple goroutines (lightweight threads in Go).

type longlivedServer struct {
	pb.UnimplementedLonglivedServer
	subscribers sync.Map
}

// This is a function parameter in Go programming language that specifies a send-only channel named "finished" of type boolean.

// The chan<- bool syntax specifies that the "finished" parameter is a channel that can only be used to send boolean values,
// but not receive them. This is useful in cases where you want to restrict the use of the channel
// to only sending values from a specific function or goroutine,
// and prevent accidental reads or blocking on the channel.

type sub struct {
	stream   pb.Longlived_SubscribeServer // stream is the server side of the RPC stream
	finished chan<- bool                  //finished is used to signal closure of a client subscribing goroutine
}

// Subscribe handles a subscribe request from a client
func (s *longlivedServer) Subscribe(request *pb.Request, stream pb.Longlived_SubscribeServer) error {
	// Handle subscribe request
	log.Printf("Received subscribe request from ID: %d", request.Id)

	//The make function is used to create channels, slices, and maps in Go.
	//In this case, make(chan bool) creates a channel that can be used to send and receive boolean values.

	fin := make(chan bool)
	// Save the subscriber stream according to the given client ID
	s.subscribers.Store(request.Id, sub{stream: stream, finished: fin})

	ctx := stream.Context()
	// Keep this scope alive because once this scope exits - the stream is closed
	for {
		select {
		case <-fin:
			log.Printf("Closing stream for client ID: %d", request.Id)
			return nil
		case <-ctx.Done():
			log.Printf("Client ID %d has disconnected", request.Id)
			return nil
		}
	}
}

// Unsubscribe handles a unsubscribe request from a client
// Note: this function is not called but it here as an example of an unary RPC for unsubscribing clients
func (s *longlivedServer) Unsubscribe(ctx context.Context, request *pb.Request) (*pb.Response, error) {
	v, ok := s.subscribers.Load(request.Id)
	if !ok {
		return nil, fmt.Errorf("failed to load subscriber key: %d", request.Id)
	}
	sub, ok := v.(sub)
	if !ok {
		return nil, fmt.Errorf("failed to cast subscriber value: %T", v)
	}
	select {
	case sub.finished <- true:
		log.Printf("Unsubscribed client: %d", request.Id)
	default:
		// Default case is to avoid blocking in case client has already unsubscribed
	}
	s.subscribers.Delete(request.Id)
	return &pb.Response{}, nil
}

func (s *longlivedServer) mockDataGenerator() {
	log.Println("Starting data generation")
	for {
		//time.Sleep(time.Second)

		// A list of clients to unsubscribe in case of error
		var unsubscribe []int32

		// Iterate over all subscribers and send data to each client
		s.subscribers.Range(func(k, v interface{}) bool {
			id, ok := k.(int32)
			if !ok {
				log.Printf("Failed to cast subscriber key: %T", k)
				return false
			}
			sub, ok := v.(sub)
			if !ok {
				log.Printf("Failed to cast subscriber value: %T", v)
				return false
			}

			// Seed the random number generator with the current time
			rand.Seed(time.Now().UnixNano())

			// Generate a random integer between 0 and 99
			randomInt := rand.Intn(100)
			// Send data over the gRPC stream to the client
			if err := sub.stream.Send(&pb.Response{Data: fmt.Sprintf("data mock for: %d", randomInt)}); err != nil {
				log.Printf("Failed to send data to client: %v", err)
				select {
				case sub.finished <- true:
					log.Printf("Unsubscribed client: %d", id)
				default:
					// Default case is to avoid blocking in case client has already unsubscribed
				}
				// In case of error the client would re-subscribe so close the subscriber stream
				unsubscribe = append(unsubscribe, id)
			}
			return true
		})

		// Unsubscribe erroneous client streams
		for _, id := range unsubscribe {
			s.subscribers.Delete(id)
		}
	}
}
