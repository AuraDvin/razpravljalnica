// Komunikacija po protokolu gRPC
// odjemalec za Razpravljalnico

package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	razpravljalnica "github.com/AuraDvin/razpravljalnica/grpc/protobufStorage"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func Client(url string) {
	// Parse the port from the URL to connect to control plane on port-1
	parts := strings.Split(url, ":")
	if len(parts) != 2 {
		panic(fmt.Sprintf("invalid URL format: %s", url))
	}

	port, err := strconv.Atoi(parts[1])
	if err != nil {
		panic(fmt.Sprintf("invalid port in URL: %s", url))
	}

	// Control plane runs on port - 1
	controlPlaneURL := fmt.Sprintf("localhost:%d", port-1)

	// First connect to control plane to discover head server
	fmt.Printf("gRPC client connecting to control plane at %v\n", controlPlaneURL)
	cpConn, err := grpc.NewClient(controlPlaneURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer cpConn.Close()

	cpClient := razpravljalnica.NewControlPlaneClient(cpConn)

	// Get head server address from control plane
	headServerInfo, err := cpClient.GetHeadServer(context.Background(), &emptypb.Empty{})
	if err != nil {
		panic(fmt.Sprintf("failed to get head server from control plane: %v", err))
	}

	fmt.Printf("Control plane returned head server at %s\n", headServerInfo.Address)

	// Connect to head server for write operations
	conn, err := grpc.NewClient(headServerInfo.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Create clients for head server (writes) and control plane (read routing)
	grpcClient := razpravljalnica.NewMessageBoardClient(conn)

	// Get tail server for read operations
	clusterState, err := cpClient.GetClusterState(context.Background(), &emptypb.Empty{})
	if err != nil {
		panic(fmt.Sprintf("failed to get cluster state: %v", err))
	}

	fmt.Printf("Cluster state: Head=%s, Tail=%s\n", clusterState.Head.Address, clusterState.Tail.Address)

	// Connect to tail server for read operations
	tailConn, err := grpc.NewClient(clusterState.Tail.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer tailConn.Close()

	tailClient := razpravljalnica.NewMessageBoardClient(tailConn)

	// Start interactive client with head (writes), tail (reads), and control plane
	runInteractiveClient(grpcClient, tailClient, cpClient)
}

func runInteractiveClient(headClient razpravljalnica.MessageBoardClient, tailClient razpravljalnica.MessageBoardClient, cpClient razpravljalnica.ControlPlaneClient) {
	reader := bufio.NewReader(os.Stdin)
	var currentUserID int64 = 0
	var subscriptionToken string
	var subscriptionServer razpravljalnica.MessageBoardClient
	var subscriptionConn *grpc.ClientConn
	var subscriptionCtx context.Context
	var subscriptionCancel context.CancelFunc

	fmt.Println("\n=== Razpravljalnica - Odjemalec ===")
	fmt.Println("Ukazi:")
	fmt.Println("  1. createuser <ime>         - Ustvari novega uporabnika")
	fmt.Println("  2. createtopic <ime>        - Ustvari novo temo")
	fmt.Println("  3. listtopics               - Seznam vseh tem")
	fmt.Println("  4. setuser <id>             - Nastavi trenutnega uporabnika")
	fmt.Println("  5. getuser <id>             - Dobi informacije o uporabniku")
	fmt.Println("  6. postmessage <topic_id> <besedilo> - Objavi sporočilo")
	fmt.Println("  7. getmessages <topic_id> [from_id] [limit] - Prejmi sporočila")
	fmt.Println("  8. likemessage <topic_id> <msg_id> - Všečkaj sporočilo")
	fmt.Println("  9. subscribe <topic_id_list> - Naroči se na teme (comma-separated)")
	fmt.Println(" 10. unsubscribe              - Prekini naročnino")
	fmt.Println(" 11. exit                    - Izhod")
	fmt.Println()

	for {
		fmt.Print("> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input == "" {
			continue
		}

		parts := strings.Fields(input)
		command := parts[0]

		switch command {
		case "exit":
			if subscriptionCancel != nil {
				subscriptionCancel()
			}
			fmt.Println("Izhod...")
			return

		case "createuser":
			if len(parts) < 2 {
				fmt.Println("Napaka: createuser <ime>")
				continue
			}
			name := strings.Join(parts[1:], " ")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			user, err := headClient.CreateUser(ctx, &razpravljalnica.CreateUserRequest{Name: name})
			cancel()
			if err != nil {
				fmt.Printf("Napaka: %v\n", err)
			} else {
				fmt.Printf("Uporabnik ustvarjen: ID=%d, Ime=%s\n", user.Id, user.Name)
			}

		case "createtopic":
			if len(parts) < 2 {
				fmt.Println("Napaka: createtopic <ime>")
				continue
			}
			name := strings.Join(parts[1:], " ")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			topic, err := headClient.CreateTopic(ctx, &razpravljalnica.CreateTopicRequest{Name: name})
			cancel()
			if err != nil {
				fmt.Printf("Napaka: %v\n", err)
			} else {
				fmt.Printf("Tema ustvarjena: ID=%d, Ime=%s\n", topic.Id, topic.Name)
			}

		case "listtopics":
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := tailClient.ListTopics(ctx, &emptypb.Empty{})
			cancel()
			if err != nil {
				fmt.Printf("Napaka: %v\n", err)
			} else {
				if len(resp.Topics) == 0 {
					fmt.Println("Ni tem.")
				} else {
					fmt.Println("Teme:")
					for _, topic := range resp.Topics {
						fmt.Printf("  %d: %s\n", topic.Id, topic.Name)
					}
				}
			}

		case "setuser":
			if len(parts) < 2 {
				fmt.Println("Napaka: setuser <id>")
				continue
			}
			id, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				fmt.Println("Napaka: Neveljavna ID")
				continue
			}
			currentUserID = id
			fmt.Printf("Trenutni uporabnik: %d\n", currentUserID)

		case "getuser":
			if len(parts) < 2 {
				fmt.Println("Napaka: getuser <id>")
				continue
			}
			id, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				fmt.Println("Napaka: Neveljavna ID")
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			user, err := tailClient.GetUser(ctx, &razpravljalnica.GetUserRequest{Id: id})
			cancel()
			if err != nil {
				fmt.Printf("Napaka: %v\n", err)
			} else {
				fmt.Printf("Uporabnik: ID=%d, Ime=%s\n", user.Id, user.Name)
			}

		case "postmessage":
			if len(parts) < 3 {
				fmt.Println("Napaka: postmessage <topic_id> <besedilo>")
				continue
			}
			topicID, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				fmt.Println("Napaka: Neveljavna tema ID")
				continue
			}
			if currentUserID == 0 {
				fmt.Println("Napaka: Najprej nastavi uporabnika z 'setuser'")
				continue
			}
			text := strings.Join(parts[2:], " ")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			msg, err := headClient.PostMessage(ctx, &razpravljalnica.PostMessageRequest{
				TopicId: topicID,
				UserId:  currentUserID,
				Text:    text,
			})
			cancel()
			if err != nil {
				fmt.Printf("Napaka: %v\n", err)
			} else {
				fmt.Printf("Sporočilo objavljeno: ID=%d\n", msg.Id)
			}

		case "getmessages":
			if len(parts) < 2 {
				fmt.Println("Napaka: getmessages <topic_id> [from_id] [limit]")
				continue
			}
			topicID, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				fmt.Println("Napaka: Neveljavna tema ID")
				continue
			}
			fromID := int64(0)
			limit := int32(100)
			if len(parts) > 2 {
				id, err := strconv.ParseInt(parts[2], 10, 64)
				if err == nil {
					fromID = id
				}
			}
			if len(parts) > 3 {
				l, err := strconv.ParseInt(parts[3], 10, 32)
				if err == nil {
					limit = int32(l)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := tailClient.GetMessages(ctx, &razpravljalnica.GetMessagesRequest{
				TopicId:       topicID,
				FromMessageId: fromID,
				Limit:         limit,
			})
			cancel()
			if err != nil {
				fmt.Printf("Napaka: %v\n", err)
			} else {
				if len(resp.Messages) == 0 {
					fmt.Println("Ni sporočil.")
				} else {
					fmt.Println("Sporočila:")
					for _, msg := range resp.Messages {
						fmt.Printf("  [%d] Uporabnik %d: %s (Všečki: %d)\n",
							msg.Id, msg.UserId, msg.Text, msg.Likes)
					}
				}
			}

		case "likemessage":
			if len(parts) < 3 {
				fmt.Println("Napaka: likemessage <topic_id> <msg_id>")
				continue
			}
			topicID, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				fmt.Println("Napaka: Neveljavna tema ID")
				continue
			}
			msgID, err := strconv.ParseInt(parts[2], 10, 64)
			if err != nil {
				fmt.Println("Napaka: Neveljavna sporočila ID")
				continue
			}
			if currentUserID == 0 {
				fmt.Println("Napaka: Najprej nastavi uporabnika z 'setuser'")
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			msg, err := headClient.LikeMessage(ctx, &razpravljalnica.LikeMessageRequest{
				TopicId:   topicID,
				MessageId: msgID,
				UserId:    currentUserID,
			})
			cancel()
			if err != nil {
				fmt.Printf("Napaka: %v\n", err)
			} else {
				fmt.Printf("Všeček dodan. Skupaj: %d\n", msg.Likes)
			}

		case "subscribe":
			if len(parts) < 2 {
				fmt.Println("Napaka: subscribe <topic_id_list>")
				continue
			}
			if currentUserID == 0 {
				fmt.Println("Napaka: Najprej nastavi uporabnika z 'setuser'")
				continue
			}

			// Razčleni seznam tem (comma-separated)
			topicIDStrs := strings.Split(parts[1], ",")
			topicIDs := make([]int64, 0)
			for _, idStr := range topicIDStrs {
				id, err := strconv.ParseInt(strings.TrimSpace(idStr), 10, 64)
				if err != nil {
					fmt.Printf("Napaka: Neveljavna tema ID: %s\n", idStr)
					continue
				}
				topicIDs = append(topicIDs, id)
			}

			if len(topicIDs) == 0 {
				fmt.Println("Napaka: Ni veljavnih tem")
				continue
			}

			// Dobi naročniško vozlišče in token
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			subResp, err := headClient.GetSubscriptionNode(ctx, &razpravljalnica.SubscriptionNodeRequest{
				UserId:  currentUserID,
				TopicId: topicIDs,
			})
			cancel()
			if err != nil {
				fmt.Printf("Napaka pri pridobivanju naročniškega vozlišča: %v\n", err)
				continue
			}

			subscriptionToken = subResp.SubscribeToken
			subscriptionServerAddr := subResp.Node.Address
			fmt.Printf("Naročen na %d tem. Povezovanje s strežnikom %s\n", len(topicIDs), subscriptionServerAddr)

			// Prekini prejšnjo naročnino, če obstaja
			if subscriptionCancel != nil {
				subscriptionCancel()
			}
			if subscriptionConn != nil {
				subscriptionConn.Close()
			}

			// Vzpostavi povezavo s strežnikom za naročnino
			subscriptionConn, err = grpc.NewClient(subscriptionServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				fmt.Printf("Napaka pri povezovanju s strežnikom za naročnino: %v\n", err)
				continue
			}

			subscriptionServer = razpravljalnica.NewMessageBoardClient(subscriptionConn)

			// Začni prejemati sporočila
			subscriptionCtx, subscriptionCancel = context.WithCancel(context.Background())
			go receiveMessages(subscriptionServer, subscriptionCtx, subscriptionToken, topicIDs[0])

		case "unsubscribe":
			if subscriptionCancel != nil {
				subscriptionCancel()
				fmt.Println("Naročnina preklicana.")
				if subscriptionConn != nil {
					subscriptionConn.Close()
					subscriptionConn = nil
				}
			} else {
				fmt.Println("Nisi naročen na nobeno temo.")
			}

		default:
			fmt.Println("Neznan ukaz. Napišite 'exit' za izhod.")
		}
	}
}

func receiveMessages(client razpravljalnica.MessageBoardClient, ctx context.Context, token string, topicID int64) {
	fmt.Println("\n[Prejemam sporočila...]")
	stream, err := client.SubscribeTopic(ctx, &razpravljalnica.SubscribeTopicRequest{
		SubscribeToken: token,
		FromMessageId:  0,
		TopicId:        []int64{topicID},
	})
	if err != nil {
		log.Printf("Napaka pri naročnini: %v\n", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		event, err := stream.Recv()
		if err == io.EOF {
			log.Println("Naročnina zaprta.")
			return
		}
		if err != nil {
			log.Printf("Napaka pri prejemanju: %v\n", err)
			return
		}

		msg := event.Message
		opType := "OBJAVA"
		if event.Op == 1 {
			opType = "VŠEČKA"
		}

		fmt.Printf("\n[%s] Uporabnik %d: %s (Všečki: %d)\n", opType, msg.UserId, msg.Text, msg.Likes)
		fmt.Print("> ")
	}
}
