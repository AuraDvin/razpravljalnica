// Komunikacija po protokolu gRPC
// strežnik za Razpravljalnico

package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/AuraDvin/razpravljalnica/grpc/protobufStorage"
	"github.com/AuraDvin/razpravljalnica/storage"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func Server(url string) {
	// pripravimo strežnik gRPC
	grpcServer := grpc.NewServer()

	// pripravimo strukturo za streženje metod MessageBoard
	messageBoardServer := NewMessageBoardServer()

	// streženje metod povežemo s strežnikom gRPC
	protobufStorage.RegisterMessageBoardServer(grpcServer, messageBoardServer)

	// izpišemo ime strežnika
	hostName, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	// odpremo vtičnico
	listener, err := net.Listen("tcp", url)
	if err != nil {
		panic(err)
	}
	fmt.Printf("gRPC server listening at %v%v\n", hostName, url)
	// začnemo s streženjem
	if err := grpcServer.Serve(listener); err != nil {
		panic(err)
	}
}

// struktura za strežnik MessageBoard
type messageBoardServer struct {
	protobufStorage.UnimplementedMessageBoardServer
	store *storage.DatabaseStorage
}

// pripravimo nov strežnik MessageBoard
func NewMessageBoardServer() *messageBoardServer {
	return &messageBoardServer{
		protobufStorage.UnimplementedMessageBoardServer{},
		storage.NewDatabaseStorage(),
	}
}

// ============================================================================
// User operations
// ============================================================================

func (s *messageBoardServer) CreateUser(ctx context.Context, req *protobufStorage.CreateUserRequest) (*protobufStorage.User, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("user name cannot be empty")
	}

	user, err := s.store.AddUser(req.Name)
	if err != nil {
		return nil, err
	}

	return &protobufStorage.User{
		Id:   user.ID,
		Name: user.Name,
	}, nil
}

func (s *messageBoardServer) GetUser(ctx context.Context, req *protobufStorage.GetUserRequest) (*protobufStorage.User, error) {
	user, err := s.store.GetUser(req.Id)
	if err != nil {
		return nil, err
	}

	return &protobufStorage.User{
		Id:   user.ID,
		Name: user.Name,
	}, nil
}

// ============================================================================
// Topic operations
// ============================================================================

func (s *messageBoardServer) CreateTopic(ctx context.Context, req *protobufStorage.CreateTopicRequest) (*protobufStorage.Topic, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("topic name cannot be empty")
	}

	topic, err := s.store.AddTopic(req.Name)
	if err != nil {
		return nil, err
	}

	return &protobufStorage.Topic{
		Id:   topic.ID,
		Name: topic.Name,
	}, nil
}

func (s *messageBoardServer) ListTopics(ctx context.Context, _ *emptypb.Empty) (*protobufStorage.ListTopicsResponse, error) {
	topics, err := s.store.ListTopics()
	if err != nil {
		return nil, err
	}

	pbTopics := make([]*protobufStorage.Topic, 0, len(topics))
	for _, topic := range topics {
		pbTopics = append(pbTopics, &protobufStorage.Topic{
			Id:   topic.ID,
			Name: topic.Name,
		})
	}

	return &protobufStorage.ListTopicsResponse{
		Topics: pbTopics,
	}, nil
}

// ============================================================================
// Message operations
// ============================================================================

func (s *messageBoardServer) PostMessage(ctx context.Context, req *protobufStorage.PostMessageRequest) (*protobufStorage.Message, error) {
	if req.Text == "" {
		return nil, fmt.Errorf("message text cannot be empty")
	}

	message, err := s.store.AddMessage(req.TopicId, req.UserId, req.Text)
	if err != nil {
		return nil, err
	}

	return &protobufStorage.Message{
		Id:        message.ID,
		TopicId:   message.TopicID,
		UserId:    message.UserID,
		Text:      message.Text,
		CreatedAt: message.CreatedAt,
		Likes:     message.Likes,
	}, nil
}

func (s *messageBoardServer) GetMessages(ctx context.Context, req *protobufStorage.GetMessagesRequest) (*protobufStorage.GetMessagesResponse, error) {
	messages, err := s.store.GetMessagesInTopic(req.TopicId, req.FromMessageId, req.Limit)
	if err != nil {
		return nil, err
	}

	pbMessages := make([]*protobufStorage.Message, 0, len(messages))
	for _, msg := range messages {
		pbMessages = append(pbMessages, &protobufStorage.Message{
			Id:        msg.ID,
			TopicId:   msg.TopicID,
			UserId:    msg.UserID,
			Text:      msg.Text,
			CreatedAt: msg.CreatedAt,
			Likes:     msg.Likes,
		})
	}

	return &protobufStorage.GetMessagesResponse{
		Messages: pbMessages,
	}, nil
}

// ============================================================================
// Like operations
// ============================================================================

func (s *messageBoardServer) LikeMessage(ctx context.Context, req *protobufStorage.LikeMessageRequest) (*protobufStorage.Message, error) {
	message, err := s.store.LikeMessage(req.TopicId, req.MessageId, req.UserId)
	if err != nil {
		return nil, err
	}

	return &protobufStorage.Message{
		Id:        message.ID,
		TopicId:   message.TopicID,
		UserId:    message.UserID,
		Text:      message.Text,
		CreatedAt: message.CreatedAt,
		Likes:     message.Likes,
	}, nil
}

// ============================================================================
// Subscription operations
// ============================================================================

func (s *messageBoardServer) GetSubscriptionNode(ctx context.Context, req *protobufStorage.SubscriptionNodeRequest) (*protobufStorage.SubscriptionNodeResponse, error) {
	// Registrira naročnino in vrne token
	token, err := s.store.RegisterSubscription(req.UserId, req.TopicId)
	if err != nil {
		return nil, err
	}

	// Vrni informacije o vozlišču (to je isto vozlišče)
	hostName, _ := os.Hostname()
	return &protobufStorage.SubscriptionNodeResponse{
		SubscribeToken: token,
		Node: &protobufStorage.NodeInfo{
			NodeId:  hostName,
			Address: "", // Odjemalec že zna naslov
		},
	}, nil
}

func (s *messageBoardServer) SubscribeTopic(req *protobufStorage.SubscribeTopicRequest, stream protobufStorage.MessageBoard_SubscribeTopicServer) error {
	// Dobi kanal za naročnino
	channel, subscription, err := s.store.GetSubscriptionChannel(req.SubscribeToken)
	if err != nil {
		return err
	}

	log.Printf("Subscription started for user %d on topics %v\n", subscription.UserID, subscription.TopicIDs)

	// Pošlji zgodovino sporočil, ki so že na voljo
	messages, err := s.store.GetMessagesInTopic(subscription.TopicIDs[0], req.FromMessageId, 100)
	if err == nil {
		for _, msg := range messages {
			event := &protobufStorage.MessageEvent{
				SequenceNumber: 0, // Zgodovina nima zaporedne številke
				Op:             protobufStorage.OpType_OP_POST,
				Message: &protobufStorage.Message{
					Id:        msg.ID,
					TopicId:   msg.TopicID,
					UserId:    msg.UserID,
					Text:      msg.Text,
					CreatedAt: msg.CreatedAt,
					Likes:     msg.Likes,
				},
				EventAt: msg.CreatedAt,
			}
			stream.Send(event)
		}
	}

	// Počakaj na nove dogodke
	for event := range channel {
		pbEvent := &protobufStorage.MessageEvent{
			SequenceNumber: event.SequenceNumber,
			Op:             protobufStorage.OpType(event.Op),
			Message: &protobufStorage.Message{
				Id:        event.Message.ID,
				TopicId:   event.Message.TopicID,
				UserId:    event.Message.UserID,
				Text:      event.Message.Text,
				CreatedAt: event.Message.CreatedAt,
				Likes:     event.Message.Likes,
			},
			EventAt: event.EventAt,
		}

		if err := stream.Send(pbEvent); err != nil {
			// Odjemalec je nepovezan
			log.Printf("Error sending event to client: %v\n", err)
			close(channel)
			break
		}
	}

	return nil
}

// func (s *serverCRUD) Subscribe(e *emptypb.Empty, stream protobufStorage.CRUD_SubscribeServer) error {
// 	// Implement this method to allow clients to subscribe to todo changes
// 	// Message TodoEvent has the Todo message T with Action, which should be a string like
// 	// "created: Dodan nov vnos v shrambo", "updated: Posodobljen vnos v shrambi", "deleted: Izbrisan vnos"
// 	// Something like when other functions, send to stream the message and which action happened
// 	s.todoStore.Subscribe()
// 	for event, ok := <-storage.ActionsChan; ok; event, ok = <-storage.ActionsChan {
// 		event_temp := &protobufStorage.TodoEvent{Action: event.Action, T: &protobufStorage.Todo{Task: event.T.Task, Completed: event.T.Completed}}
// 		err := stream.Send(&protobufStorage.TodoEvent{Action: event_temp.Action, T: event_temp.T})
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	// s.todoStore.Unsubscribe()
// 	return nil
// }
