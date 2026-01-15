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
	// vzpostavimo povezavo s strežnikom
	fmt.Printf("gRPC client connecting to %v\n", url)
	// conn, err := grpc.Dial(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// vzpostavimo vmesnik gRPC
	grpcClient := razpravljalnica.NewMessageBoardClient(conn)

	// Začetek interaktivne lupine
	runInteractiveClient(grpcClient)
}

func runInteractiveClient(client razpravljalnica.MessageBoardClient) {
	reader := bufio.NewReader(os.Stdin)
	var currentUserID int64 = 0
	var subscriptionToken string
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
			user, err := client.CreateUser(ctx, &razpravljalnica.CreateUserRequest{Name: name})
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
			topic, err := client.CreateTopic(ctx, &razpravljalnica.CreateTopicRequest{Name: name})
			cancel()
			if err != nil {
				fmt.Printf("Napaka: %v\n", err)
			} else {
				fmt.Printf("Tema ustvarjena: ID=%d, Ime=%s\n", topic.Id, topic.Name)
			}

		case "listtopics":
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := client.ListTopics(ctx, &emptypb.Empty{})
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
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			user, err := client.GetUser(ctx, &razpravljalnica.GetUserRequest{Id: id})
			cancel()
			if err != nil {
				fmt.Printf("Napaka: %v\n", err)
			} else {
				fmt.Printf("Trenutni uporabnik: %s (%d)\n", user.Name, user.Id)
				currentUserID = id
			}
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
			user, err := client.GetUser(ctx, &razpravljalnica.GetUserRequest{Id: id})
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
			msg, err := client.PostMessage(ctx, &razpravljalnica.PostMessageRequest{
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
			resp, err := client.GetMessages(ctx, &razpravljalnica.GetMessagesRequest{
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
					usernames := make(map[int64]string)
					for _, msg := range resp.Messages {
						val, ok := usernames[msg.UserId]
						if !ok {
							user, err := getUser(msg.UserId, client)
							if err != nil {
								val = ""
							} else {
								val = user.Name
							}
						}
						fmt.Printf("  [%d] Uporabnik '%s' %d: %s (Všečki: %d)\n",
							msg.Id, val, msg.UserId, msg.Text, msg.Likes)
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
			msg, err := client.LikeMessage(ctx, &razpravljalnica.LikeMessageRequest{
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
				fmt.Println("Napaka: subscribe <topic_id>[,<topic_id>]*")
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
			subResp, err := client.GetSubscriptionNode(ctx, &razpravljalnica.SubscriptionNodeRequest{
				UserId:  currentUserID,
				TopicId: topicIDs,
			})
			cancel()
			if err != nil {
				fmt.Printf("Napaka: %v\n", err)
				continue
			}

			subscriptionToken = subResp.SubscribeToken
			fmt.Printf("Naročen na %d tem. Token: %s\n", len(topicIDs), subscriptionToken[:20]+"...")

			// Prekini prejšnjo naročnino, če obstaja
			if subscriptionCancel != nil {
				subscriptionCancel()
			}

			// Začni prejemati sporočila
			subscriptionCtx, subscriptionCancel = context.WithCancel(context.Background())
			go receiveMessages(client, subscriptionCtx, subscriptionToken, topicIDs[0])

		case "unsubscribe":
			if subscriptionCancel != nil {
				subscriptionCancel()
				fmt.Println("Naročnina preklicana.")
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

func getUser(id int64, client razpravljalnica.MessageBoardClient) (razpravljalnica.User, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	user, err := client.GetUser(ctx, &razpravljalnica.GetUserRequest{Id: id})
	cancel()
	return razpravljalnica.User{Id: user.Id, Name: user.Name}, err
}
