// Komunikacija po protokolu gRPC
// strežnik za Razpravljalnico

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	protobufStorage "github.com/AuraDvin/razpravljalnica/grpc/protobufStorage"
	"github.com/AuraDvin/razpravljalnica/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ============================================================================
// Control Plane State Management
// ============================================================================

type ServerNode struct {
	Address               string
	Role                  protobufStorage.ServerRole
	ActiveSubscriptions   int64
	NextServerAddress     string
	NextServerConn        *grpc.ClientConn
	NextServerChainClient protobufStorage.ChainReplicationClient
}

// Global control plane state (synchronized by mutex)
var (
	cpMutex           sync.RWMutex
	serverRegistry    map[string]*ServerNode // address → ServerNode
	headServerAddress string
	tailServerAddress string
)

func initControlPlane() {
	cpMutex.Lock()
	defer cpMutex.Unlock()
	serverRegistry = make(map[string]*ServerNode)
}

// RegisterServer registers a server with the control plane
func (s *controlPlaneServer) RegisterServer(ctx context.Context, req *protobufStorage.ServerRegistration) (*emptypb.Empty, error) {
	cpMutex.Lock()
	defer cpMutex.Unlock()

	log.Printf("Registering server %s with role %v\n", req.ServerAddress, req.Role)

	node := &ServerNode{
		Address:             req.ServerAddress,
		Role:                req.Role,
		ActiveSubscriptions: 0,
	}

	serverRegistry[req.ServerAddress] = node

	// Update head and tail addresses
	for addr, n := range serverRegistry {
		if n.Role == protobufStorage.ServerRole_HEAD {
			headServerAddress = addr
		}
		if n.Role == protobufStorage.ServerRole_TAIL {
			tailServerAddress = addr
		}
	}

	return &emptypb.Empty{}, nil
}

// GetHeadServer returns the head server address
func (s *controlPlaneServer) GetHeadServer(ctx context.Context, _ *emptypb.Empty) (*protobufStorage.ServerInfo, error) {
	cpMutex.RLock()
	defer cpMutex.RUnlock()

	if headServerAddress == "" {
		return nil, fmt.Errorf("no head server registered")
	}

	node := serverRegistry[headServerAddress]
	return &protobufStorage.ServerInfo{
		Address: node.Address,
		Role:    node.Role,
	}, nil
}

// GetSubscriptionServer returns the server with the least subscriptions
func (s *controlPlaneServer) GetSubscriptionServer(ctx context.Context, _ *emptypb.Empty) (*protobufStorage.ServerInfo, error) {
	cpMutex.RLock()
	defer cpMutex.RUnlock()

	if len(serverRegistry) == 0 {
		return nil, fmt.Errorf("no servers registered")
	}

	// Find server with least subscriptions
	var selectedNode *ServerNode
	var minSubs int64 = int64(^uint64(0) >> 1) // max int64

	for _, node := range serverRegistry {
		if node.ActiveSubscriptions < minSubs {
			minSubs = node.ActiveSubscriptions
			selectedNode = node
		}
	}

	if selectedNode == nil {
		return nil, fmt.Errorf("no server found")
	}

	return &protobufStorage.ServerInfo{
		Address: selectedNode.Address,
		Role:    selectedNode.Role,
	}, nil
}

// ReportSubscriptionCount updates the subscription count for a server
func (s *controlPlaneServer) ReportSubscriptionCount(ctx context.Context, req *protobufStorage.ServerStatus) (*emptypb.Empty, error) {
	cpMutex.Lock()
	defer cpMutex.Unlock()

	if node, exists := serverRegistry[req.ServerAddress]; exists {
		node.ActiveSubscriptions = req.ActiveSubscriptions
		log.Printf("Server %s subscription count updated to %d\n", req.ServerAddress, req.ActiveSubscriptions)
	}

	return &emptypb.Empty{}, nil
}

// Control plane server implementation
type controlPlaneServer struct {
	protobufStorage.UnimplementedControlPlaneServer
	headServer *messageBoardServer // Reference to the head server for subscription registration
}

func (s *controlPlaneServer) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*protobufStorage.GetClusterStateResponse, error) {
	cpMutex.RLock()
	defer cpMutex.RUnlock()

	resp := &protobufStorage.GetClusterStateResponse{}

	for _, node := range serverRegistry {
		if node.Role == protobufStorage.ServerRole_HEAD {
			resp.Head = &protobufStorage.NodeInfo{
				NodeId:  node.Address,
				Address: node.Address,
			}
		}
		if node.Role == protobufStorage.ServerRole_TAIL {
			resp.Tail = &protobufStorage.NodeInfo{
				NodeId:  node.Address,
				Address: node.Address,
			}
		}
	}

	return resp, nil
}

func (s *controlPlaneServer) GetSubscriptionNode(ctx context.Context, req *protobufStorage.SubscriptionNodeRequest) (*protobufStorage.SubscriptionNodeResponse, error) {
	if s.headServer == nil {
		return nil, fmt.Errorf("head server not initialized")
	}

	// Get load-balanced subscription server from control plane
	subsServer, err := s.GetSubscriptionServer(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, fmt.Errorf("failed to get subscription server: %v", err)
	}

	// Register subscription on the head server's store
	token, err := s.headServer.store.RegisterSubscription(req.UserId, req.TopicId)
	if err != nil {
		return nil, err
	}

	// Report subscription count to the assigned subscription server
	go func() {
		subsNode := serverRegistry[subsServer.Address]
		if subsNode != nil {
			cpMutex.Lock()
			subsNode.ActiveSubscriptions++
			count := subsNode.ActiveSubscriptions
			cpMutex.Unlock()

			_, _ = s.ReportSubscriptionCount(context.Background(), &protobufStorage.ServerStatus{
				ServerAddress:       subsServer.Address,
				ActiveSubscriptions: count,
			})
		}
	}()

	return &protobufStorage.SubscriptionNodeResponse{
		SubscribeToken: token,
		Node: &protobufStorage.NodeInfo{
			NodeId:  subsServer.Address,
			Address: subsServer.Address,
		},
	}, nil
}

func StartServerChain(basePort int, numServers int) {
	// Setup logging to file in logs directory
	logFile, err := os.OpenFile("logs/server.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open log file: %v\n", err)
		return
	}
	defer logFile.Close()

	// Create a multi-writer that writes to both stdout and file
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(multiWriter)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Printf("New Session ================================\n")
	initControlPlane()

	// Start control plane server on a dedicated port (basePort - 1)
	controlPlanePort := basePort - 1
	controlPlaneURL := fmt.Sprintf("localhost:%d", controlPlanePort)

	cpServer := &controlPlaneServer{}
	grpcCPServer := grpc.NewServer()
	protobufStorage.RegisterControlPlaneServer(grpcCPServer, cpServer)

	go func() {
		listener, err := net.Listen("tcp", controlPlaneURL)
		if err != nil {
			log.Fatalf("failed to listen for control plane on %s: %v", controlPlaneURL, err)
		}
		fmt.Printf("Control plane listening at %s\n", controlPlaneURL)
		if err := grpcCPServer.Serve(listener); err != nil {
			log.Fatalf("failed to serve control plane: %v", err)
		}
	}()

	var servers []*grpc.Server
	var mbServers []*messageBoardServer

	// Create all servers in the chain
	for i := 0; i < numServers; i++ {
		port := basePort + i
		url := fmt.Sprintf("localhost:%d", port)

		// Create gRPC server
		grpcServer := grpc.NewServer()
		servers = append(servers, grpcServer)

		// Create message board server
		mbs := NewMessageBoardServer()
		mbs.serverAddress = url

		// Determine role based on position
		if i == 0 {
			mbs.role = protobufStorage.ServerRole_HEAD
			// Set the head server reference on control plane
			cpServer.headServer = mbs
		} else if i == numServers-1 {
			mbs.role = protobufStorage.ServerRole_TAIL
		} else {
			mbs.role = protobufStorage.ServerRole_MIDDLE
		}

		// Set up control plane client
		cpConn, err := grpc.NewClient(controlPlaneURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("failed to connect to control plane: %v", err)
		}
		mbs.controlPlaneClient = protobufStorage.NewControlPlaneClient(cpConn)

		// If not the last server, set next server URL
		if i < numServers-1 {
			nextPort := basePort + i + 1
			nextURL := fmt.Sprintf("localhost:%d", nextPort)
			mbs.nextServerURL = nextURL
		}

		mbServers = append(mbServers, mbs)

		// Register the services
		protobufStorage.RegisterMessageBoardServer(grpcServer, mbs)

		// Register ChainReplication service
		chainReplServer := &chainReplicationServer{messageBoardServer: mbs}
		protobufStorage.RegisterChainReplicationServer(grpcServer, chainReplServer)

		// Start the server in a goroutine
		go func(grpcServer *grpc.Server, url string, idx int, server *messageBoardServer) {
			listener, err := net.Listen("tcp", url)
			if err != nil {
				log.Fatalf("failed to listen on %s: %v", url, err)
			}
			fmt.Printf("Server %d (role: %v) listening at %s\n", idx, server.role, url)

			// Register server with control plane
			time.Sleep(50 * time.Millisecond) // Ensure control plane is ready
			_, err = server.controlPlaneClient.RegisterServer(context.Background(), &protobufStorage.ServerRegistration{
				ServerAddress: url,
				Role:          server.role,
			})
			if err != nil {
				log.Printf("failed to register server %s with control plane: %v", url, err)
			}

			// If not the last server, connect to the next one after a short delay
			if server.nextServerURL != "" {
				go func() {
					time.Sleep(200 * time.Millisecond)
					conn, err := grpc.NewClient(server.nextServerURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
					if err != nil {
						log.Printf("failed to connect to next server %s: %v", server.nextServerURL, err)
						return
					}
					server.nextServerConn = conn
					server.nextServerChainClient = protobufStorage.NewChainReplicationClient(conn)
					fmt.Printf("Server %d connected to chain client at %s\n", idx, server.nextServerURL)
				}()
			}

			if err := grpcServer.Serve(listener); err != nil {
				log.Fatalf("failed to serve on %s: %v", url, err)
			}
		}(grpcServer, url, i, mbs)
	}

	// Keep the program running
	select {}
}

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
	store                   *storage.DatabaseStorage
	role                    protobufStorage.ServerRole
	serverAddress           string
	controlPlaneClient      protobufStorage.ControlPlaneClient
	nextServerURL           string // URL of the next server in the chain (empty if last server)
	nextServerConn          *grpc.ClientConn
	nextServerChainClient   protobufStorage.ChainReplicationClient
	activeSubscriptionCount int64
	subscriptionCountMutex  sync.Mutex
}

// pripravimo nov strežnik MessageBoard
func NewMessageBoardServer() *messageBoardServer {
	return &messageBoardServer{
		UnimplementedMessageBoardServer: protobufStorage.UnimplementedMessageBoardServer{},
		store:                           storage.NewDatabaseStorage(),
		role:                            protobufStorage.ServerRole_MIDDLE,
		serverAddress:                   "",
		controlPlaneClient:              nil,
		nextServerURL:                   "",
		nextServerConn:                  nil,
		nextServerChainClient:           nil,
		activeSubscriptionCount:         0,
	}
}

// ============================================================================
// Chain Replication Implementation
// ============================================================================

type chainReplicationServer struct {
	protobufStorage.UnimplementedChainReplicationServer
	messageBoardServer *messageBoardServer
}

// replicateWrite sends a write operation to the next server with retry logic
func (s *messageBoardServer) replicateWrite(ctx context.Context, req *protobufStorage.ReplicateWriteRequest) (*protobufStorage.ReplicateWriteAck, error) {
	// If this is the tail server, acknowledge immediately
	if s.nextServerChainClient == nil {
		log.Printf("[%s] Tail server confirming sequence %d (op=%v)\n", s.serverAddress, req.SequenceNumber, req.Op)
		return &protobufStorage.ReplicateWriteAck{
			SequenceNumber: req.SequenceNumber,
			Success:        true,
		}, nil
	}

	// Retry logic with exponential backoff: 3 attempts, 100ms → 200ms → 400ms
	backoffDelays := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		// Call the next server in the chain
		log.Printf("[%s] Forwarding sequence %d to next server (attempt %d/3)\n", s.serverAddress, req.SequenceNumber, attempt+1)
		ack, err := s.nextServerChainClient.ReplicateWrite(ctx, req)
		if err == nil {
			if ack.Success {
				log.Printf("[%s] Received ACK for sequence %d from next server\n", s.serverAddress, req.SequenceNumber)
			} else {
				log.Printf("[%s] Received NACK for sequence %d from next server: %s\n", s.serverAddress, req.SequenceNumber, ack.ErrorMessage)
			}
			return ack, nil
		}

		lastErr = err
		log.Printf("[%s] Replication attempt %d failed for sequence %d: %v\n", s.serverAddress, attempt+1, req.SequenceNumber, err)

		// Wait before retrying (except on last attempt)
		if attempt < 2 {
			time.Sleep(backoffDelays[attempt])
		}
	}

	// All retries failed
	log.Printf("[%s] All replication attempts failed for sequence %d\n", s.serverAddress, req.SequenceNumber)
	return &protobufStorage.ReplicateWriteAck{
		SequenceNumber: req.SequenceNumber,
		Success:        false,
		ErrorMessage:   fmt.Sprintf("replication failed after 3 attempts: %v", lastErr),
	}, lastErr
}

// ReplicateWrite implements the ChainReplication service
func (s *chainReplicationServer) ReplicateWrite(ctx context.Context, req *protobufStorage.ReplicateWriteRequest) (*protobufStorage.ReplicateWriteAck, error) {
	log.Printf("[%s] Received replication request: seq=%d, op=%v\n", s.messageBoardServer.serverAddress, req.SequenceNumber, req.Op)

	// Forward to next server if not tail
	ack, err := s.messageBoardServer.replicateWrite(ctx, req)
	if err != nil {
		log.Printf("[%s] Error during replication for seq=%d: %v\n", s.messageBoardServer.serverAddress, req.SequenceNumber, err)
		return ack, err
	}

	// If downstream acknowledged, apply the operation locally
	if ack.Success {
		log.Printf("[%s] Applying operation locally for seq=%d\n", s.messageBoardServer.serverAddress, req.SequenceNumber)
		switch req.Op {
		case protobufStorage.OpType_OP_POST:
			if req.Message != nil {
				// Apply the post operation
				_, storeErr := s.messageBoardServer.store.AddMessage(req.Message.TopicId, req.Message.UserId, req.Message.Text)
				if storeErr != nil {
					log.Printf("[%s] Failed to store message for seq=%d: %v\n", s.messageBoardServer.serverAddress, req.SequenceNumber, storeErr)
					return &protobufStorage.ReplicateWriteAck{
						SequenceNumber: req.SequenceNumber,
						Success:        false,
						ErrorMessage:   fmt.Sprintf("failed to store message: %v", storeErr),
					}, nil
				}
				log.Printf("[%s] Successfully stored message for seq=%d\n", s.messageBoardServer.serverAddress, req.SequenceNumber)
			}

		case protobufStorage.OpType_OP_LIKE:
			if req.Message != nil {
				// Apply the like operation
				_, storeErr := s.messageBoardServer.store.LikeMessage(req.Message.TopicId, req.Message.Id, req.Message.UserId)
				if storeErr != nil {
					log.Printf("[%s] Failed to apply like for seq=%d: %v\n", s.messageBoardServer.serverAddress, req.SequenceNumber, storeErr)
					return &protobufStorage.ReplicateWriteAck{
						SequenceNumber: req.SequenceNumber,
						Success:        false,
						ErrorMessage:   fmt.Sprintf("failed to apply like: %v", storeErr),
					}, nil
				}
				log.Printf("[%s] Successfully applied like for seq=%d\n", s.messageBoardServer.serverAddress, req.SequenceNumber)
			}

		case protobufStorage.OpType_OP_CREATE_USER:
			if req.Message != nil {
				// Apply the create user operation
				_, storeErr := s.messageBoardServer.store.AddUser(req.Message.Text)
				if storeErr != nil {
					log.Printf("[%s] Failed to create user for seq=%d: %v\n", s.messageBoardServer.serverAddress, req.SequenceNumber, storeErr)
					return &protobufStorage.ReplicateWriteAck{
						SequenceNumber: req.SequenceNumber,
						Success:        false,
						ErrorMessage:   fmt.Sprintf("failed to create user: %v", storeErr),
					}, nil
				}
				log.Printf("[%s] Successfully created user for seq=%d\n", s.messageBoardServer.serverAddress, req.SequenceNumber)
			}

		case protobufStorage.OpType_OP_CREATE_TOPIC:
			if req.Message != nil {
				// Apply the create topic operation
				_, storeErr := s.messageBoardServer.store.AddTopic(req.Message.Text)
				if storeErr != nil {
					log.Printf("[%s] Failed to create topic for seq=%d: %v\n", s.messageBoardServer.serverAddress, req.SequenceNumber, storeErr)
					return &protobufStorage.ReplicateWriteAck{
						SequenceNumber: req.SequenceNumber,
						Success:        false,
						ErrorMessage:   fmt.Sprintf("failed to create topic: %v", storeErr),
					}, nil
				}
				log.Printf("[%s] Successfully created topic for seq=%d\n", s.messageBoardServer.serverAddress, req.SequenceNumber)
			}
		}
	} else {
		log.Printf("[%s] Skipping local application - downstream failed for seq=%d: %s\n", s.messageBoardServer.serverAddress, req.SequenceNumber, ack.ErrorMessage)
	}

	log.Printf("[%s] Sending confirmation for seq=%d (success=%v)\n", s.messageBoardServer.serverAddress, req.SequenceNumber, ack.Success)
	return ack, nil
}

// ============================================================================
// User operations
// ============================================================================

func (s *messageBoardServer) CreateUser(ctx context.Context, req *protobufStorage.CreateUserRequest) (*protobufStorage.User, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("user name cannot be empty")
	}

	// Only HEAD can receive write requests
	if s.role != protobufStorage.ServerRole_HEAD {
		return nil, fmt.Errorf("write operations only allowed on head server")
	}

	// Create the user locally
	user, err := s.store.AddUser(req.Name)
	if err != nil {
		return nil, err
	}

	// Replicate the write through the chain
	replicateReq := &protobufStorage.ReplicateWriteRequest{
		SequenceNumber: user.ID, // Use user ID as sequence number
		Op:             protobufStorage.OpType_OP_CREATE_USER,
		Message: &protobufStorage.Message{
			Id:   user.ID,
			Text: user.Name, // Store user name in text field temporarily
		},
		Timestamp: timestamppb.Now(),
	}

	// Replicate to next server
	ack, err := s.replicateWrite(ctx, replicateReq)
	if err != nil || !ack.Success {
		errMsg := fmt.Sprintf("replication failed: %v", err)
		if ack != nil && ack.ErrorMessage != "" {
			errMsg = ack.ErrorMessage
		}
		return nil, fmt.Errorf(errMsg)
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

	// Only HEAD can receive write requests
	if s.role != protobufStorage.ServerRole_HEAD {
		return nil, fmt.Errorf("write operations only allowed on head server")
	}

	// Create the topic locally
	topic, err := s.store.AddTopic(req.Name)
	if err != nil {
		return nil, err
	}

	// Replicate the write through the chain
	replicateReq := &protobufStorage.ReplicateWriteRequest{
		SequenceNumber: topic.ID, // Use topic ID as sequence number
		Op:             protobufStorage.OpType_OP_CREATE_TOPIC,
		Message: &protobufStorage.Message{
			Id:   topic.ID,
			Text: topic.Name, // Store topic name in text field temporarily
		},
		Timestamp: timestamppb.Now(),
	}

	// Replicate to next server
	ack, err := s.replicateWrite(ctx, replicateReq)
	if err != nil || !ack.Success {
		errMsg := fmt.Sprintf("replication failed: %v", err)
		if ack != nil && ack.ErrorMessage != "" {
			errMsg = ack.ErrorMessage
		}
		return nil, fmt.Errorf(errMsg)
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

	// Only HEAD can receive write requests
	if s.role != protobufStorage.ServerRole_HEAD {
		return nil, fmt.Errorf("write operations only allowed on head server")
	}

	// Create the message locally
	message, err := s.store.AddMessage(req.TopicId, req.UserId, req.Text)
	if err != nil {
		return nil, err
	}

	// Replicate the write through the chain
	replicateReq := &protobufStorage.ReplicateWriteRequest{
		SequenceNumber: message.ID, // Use message ID as sequence number
		Op:             protobufStorage.OpType_OP_POST,
		Message: &protobufStorage.Message{
			Id:        message.ID,
			TopicId:   message.TopicID,
			UserId:    message.UserID,
			Text:      message.Text,
			CreatedAt: message.CreatedAt,
			Likes:     message.Likes,
		},
		Timestamp: message.CreatedAt,
	}

	// Replicate to next server
	ack, err := s.replicateWrite(ctx, replicateReq)
	if err != nil || !ack.Success {
		errMsg := fmt.Sprintf("replication failed: %v", err)
		if ack != nil && ack.ErrorMessage != "" {
			errMsg = ack.ErrorMessage
		}
		// Don't return the message if replication failed
		return nil, fmt.Errorf(errMsg)
	}

	// Return the message only after successful replication
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
	// Only HEAD can receive write requests
	if s.role != protobufStorage.ServerRole_HEAD {
		return nil, fmt.Errorf("write operations only allowed on head server")
	}

	message, err := s.store.LikeMessage(req.TopicId, req.MessageId, req.UserId)
	if err != nil {
		return nil, err
	}

	// Replicate the write through the chain
	replicateReq := &protobufStorage.ReplicateWriteRequest{
		SequenceNumber: req.MessageId, // Use message ID as sequence number
		Op:             protobufStorage.OpType_OP_LIKE,
		Message: &protobufStorage.Message{
			Id:        message.ID,
			TopicId:   message.TopicID,
			UserId:    message.UserID,
			Text:      message.Text,
			CreatedAt: message.CreatedAt,
			Likes:     message.Likes,
		},
		Timestamp: message.CreatedAt,
	}

	// Replicate to next server
	ack, err := s.replicateWrite(ctx, replicateReq)
	if err != nil || !ack.Success {
		errMsg := fmt.Sprintf("replication failed: %v", err)
		if ack != nil && ack.ErrorMessage != "" {
			errMsg = ack.ErrorMessage
		}
		// Don't return the message if replication failed
		return nil, fmt.Errorf(errMsg)
	}

	// Return the message only after successful replication
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

func (s *messageBoardServer) SubscribeTopic(req *protobufStorage.SubscribeTopicRequest, stream protobufStorage.MessageBoard_SubscribeTopicServer) error {
	// Dobi kanal za naročnino
	channel, subscription, err := s.store.GetSubscriptionChannel(req.SubscribeToken)
	if err != nil {
		return err
	}

	log.Printf("Subscription started for user %d on topics %v\n", subscription.UserID, subscription.TopicIDs)

	// Defer cleanup: decrement subscription count when subscription ends
	defer func() {
		s.subscriptionCountMutex.Lock()
		s.activeSubscriptionCount--
		count := s.activeSubscriptionCount
		s.subscriptionCountMutex.Unlock()

		_, _ = s.controlPlaneClient.ReportSubscriptionCount(context.Background(), &protobufStorage.ServerStatus{
			ServerAddress:       s.serverAddress,
			ActiveSubscriptions: count,
		})
		log.Printf("Subscription ended, decremented count to %d\n", count)
	}()

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

	for {
		select {
		// Preklici izvajanje ce ni Context-a
		case <-stream.Context().Done():
			log.Printf("Client disconnected (unsubscribe)")
			return nil
		// Ujemi nove dogodke
		case event := <-channel:
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
				return nil
			}
		}
	}
}
