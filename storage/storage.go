package storage

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

type User struct {
	ID   int64
	Name string
}

type Topic struct {
	ID   int64
	Name string
}

type Message struct {
	ID        int64
	TopicID   int64
	UserID    int64
	Text      string
	CreatedAt *timestamppb.Timestamp
	Likes     int32
}

type Subscription struct {
	TopicIDs []int64
	UserID   int64
	Channel  chan *MessageEvent
}

type MessageEvent struct {
	SequenceNumber int64
	Op             int32 // 0 = OP_POST, 1 = OP_LIKE
	Message        *Message
	EventAt        *timestamppb.Timestamp
}

type DatabaseStorage struct {
	lock          sync.RWMutex
	users         map[int64]*User
	topics        map[int64]*Topic
	messages      map[int64]*Message
	subscriptions map[string]*Subscription // keyed by token

	nextUserID    int64
	nextTopicID   int64
	nextMessageID int64
	nextSeqNum    int64

	// Kanali za distribucijo dogodkov
	eventChannels []chan *MessageEvent
	eventLock     sync.RWMutex
}

var (
	ErrorUserNotFound    = errors.New("user not found")
	ErrorTopicNotFound   = errors.New("topic not found")
	ErrorMessageNotFound = errors.New("message not found")
	ErrorInvalidToken    = errors.New("invalid subscription token")
)

func NewDatabaseStorage() *DatabaseStorage {
	return &DatabaseStorage{
		users:         make(map[int64]*User),
		topics:        make(map[int64]*Topic),
		messages:      make(map[int64]*Message),
		subscriptions: make(map[string]*Subscription),
		eventChannels: make([]chan *MessageEvent, 0),
		nextUserID:    1,
		nextTopicID:   1,
		nextMessageID: 1,
		nextSeqNum:    1,
	}
}

// ============================================================================
// User operations
// ============================================================================

func (db *DatabaseStorage) AddUser(name string) (*User, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	user := &User{
		ID:   db.nextUserID,
		Name: name,
	}
	db.users[user.ID] = user
	db.nextUserID++

	return user, nil
}

func (db *DatabaseStorage) GetUser(id int64) (*User, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	user, ok := db.users[id]
	if !ok {
		return nil, ErrorUserNotFound
	}
	return user, nil
}

// ============================================================================
// Topic operations
// ============================================================================

func (db *DatabaseStorage) AddTopic(name string) (*Topic, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	topic := &Topic{
		ID:   db.nextTopicID,
		Name: name,
	}
	db.topics[topic.ID] = topic
	db.nextTopicID++

	return topic, nil
}

func (db *DatabaseStorage) GetTopic(id int64) (*Topic, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	topic, ok := db.topics[id]
	if !ok {
		return nil, ErrorTopicNotFound
	}
	return topic, nil
}

func (db *DatabaseStorage) ListTopics() ([]*Topic, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	topics := make([]*Topic, 0, len(db.topics))
	for _, topic := range db.topics {
		topics = append(topics, topic)
	}
	return topics, nil
}

// ============================================================================
// Message operations
// ============================================================================

func (db *DatabaseStorage) AddMessage(topicID int64, userID int64, text string) (*Message, error) {
	db.lock.Lock()

	// Preveri ali obstajata tema in uporabnik
	if _, ok := db.topics[topicID]; !ok {
		db.lock.Unlock()
		return nil, ErrorTopicNotFound
	}
	if _, ok := db.users[userID]; !ok {
		db.lock.Unlock()
		return nil, ErrorUserNotFound
	}

	message := &Message{
		ID:        db.nextMessageID,
		TopicID:   topicID,
		UserID:    userID,
		Text:      text,
		CreatedAt: timestamppb.Now(),
		Likes:     0,
	}

	db.messages[message.ID] = message
	db.nextMessageID++
	seqNum := db.nextSeqNum
	db.nextSeqNum++

	db.lock.Unlock()

	// Pošlji dogodek naročnikom
	db.broadcastEvent(&MessageEvent{
		SequenceNumber: seqNum,
		Op:             0, // OP_POST
		Message:        message,
		EventAt:        timestamppb.Now(),
	})

	return message, nil
}

func (db *DatabaseStorage) GetMessage(id int64) (*Message, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	message, ok := db.messages[id]
	if !ok {
		return nil, ErrorMessageNotFound
	}
	return message, nil
}

func (db *DatabaseStorage) GetMessagesInTopic(topicID int64, fromMessageID int64, limit int32) ([]*Message, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	// Preveri ali obstaja tema
	if _, ok := db.topics[topicID]; !ok {
		return nil, ErrorTopicNotFound
	}

	var messages []*Message
	for _, msg := range db.messages {
		if msg.TopicID == topicID && msg.ID >= fromMessageID {
			messages = append(messages, msg)
		}
	}

	// Sortiraj po ID-ju
	if len(messages) > 0 {
		for i := 0; i < len(messages)-1; i++ {
			for j := i + 1; j < len(messages); j++ {
				if messages[i].ID > messages[j].ID {
					messages[i], messages[j] = messages[j], messages[i]
				}
			}
		}
	}

	// Omejitev rezultatov
	if limit > 0 && int32(len(messages)) > limit {
		messages = messages[:limit]
	}

	return messages, nil
}

// ============================================================================
// Like operations
// ============================================================================

func (db *DatabaseStorage) LikeMessage(topicID int64, messageID int64, userID int64) (*Message, error) {
	db.lock.Lock()

	// Preveri ali obstajata tema in sporočilo
	if _, ok := db.topics[topicID]; !ok {
		db.lock.Unlock()
		return nil, ErrorTopicNotFound
	}

	message, ok := db.messages[messageID]
	if !ok || message.TopicID != topicID {
		db.lock.Unlock()
		return nil, ErrorMessageNotFound
	}

	// Preveri ali obstaja uporabnik
	if _, ok := db.users[userID]; !ok {
		db.lock.Unlock()
		return nil, ErrorUserNotFound
	}

	// Povečaj število všečkov
	message.Likes++
	seqNum := db.nextSeqNum
	db.nextSeqNum++

	// Naredite kopijo sporočila za pošiljanje
	messageCopy := &Message{
		ID:        message.ID,
		TopicID:   message.TopicID,
		UserID:    message.UserID,
		Text:      message.Text,
		CreatedAt: message.CreatedAt,
		Likes:     message.Likes,
	}

	db.lock.Unlock()

	// Pošlji dogodek naročnikom
	db.broadcastEvent(&MessageEvent{
		SequenceNumber: seqNum,
		Op:             1, // OP_LIKE
		Message:        messageCopy,
		EventAt:        timestamppb.Now(),
	})

	return messageCopy, nil
}

// ============================================================================
// Subscription operations
// ============================================================================

func (db *DatabaseStorage) RegisterSubscription(userID int64, topicIDs []int64) (string, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Preveri ali obstaja uporabnik
	if _, ok := db.users[userID]; !ok {
		return "", ErrorUserNotFound
	}

	// Preveri ali obstajajo vse teme
	for _, topicID := range topicIDs {
		if _, ok := db.topics[topicID]; !ok {
			return "", ErrorTopicNotFound
		}
	}

	// Ustvari token
	token := generateToken(userID)

	// Ustvari kanal za napake
	channel := make(chan *MessageEvent, 100)

	subscription := &Subscription{
		TopicIDs: topicIDs,
		UserID:   userID,
		Channel:  channel,
	}

	db.subscriptions[token] = subscription

	// Dodaj kanal v seznam za oddajo dogodkov
	db.eventLock.Lock()
	db.eventChannels = append(db.eventChannels, channel)
	db.eventLock.Unlock()

	return token, nil
}

// RegisterSubscriptionWithToken registers a subscription with a specific token (for replication)
func (db *DatabaseStorage) RegisterSubscriptionWithToken(token string, userID int64, topicIDs []int64) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Preveri ali obstaja uporabnik
	if _, ok := db.users[userID]; !ok {
		return ErrorUserNotFound
	}

	// Preveri ali obstajajo vse teme
	for _, topicID := range topicIDs {
		if _, ok := db.topics[topicID]; !ok {
			return ErrorTopicNotFound
		}
	}

	// Ustvari kanal
	channel := make(chan *MessageEvent, 100)

	subscription := &Subscription{
		TopicIDs: topicIDs,
		UserID:   userID,
		Channel:  channel,
	}

	db.subscriptions[token] = subscription

	// Dodaj kanal v seznam za oddajo dogodkov
	db.eventLock.Lock()
	db.eventChannels = append(db.eventChannels, channel)
	db.eventLock.Unlock()

	return nil
}

func (db *DatabaseStorage) GetSubscriptionChannel(token string) (chan *MessageEvent, *Subscription, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	sub, ok := db.subscriptions[token]
	if !ok {
		return nil, nil, ErrorInvalidToken
	}

	return sub.Channel, sub, nil
}

// ============================================================================
// Event broadcasting
// ============================================================================

func (db *DatabaseStorage) broadcastEvent(event *MessageEvent) {
	db.lock.RLock()
	subscriptions := make(map[string]*Subscription)
	for token, sub := range db.subscriptions {
		subscriptions[token] = sub
	}
	db.lock.RUnlock()

	// Pošlji dogodek vsem naročnikom, ki so naročeni na to temo
	for _, sub := range subscriptions {
		for _, topicID := range sub.TopicIDs {
			if topicID == event.Message.TopicID {
				select {
				case sub.Channel <- event:
				case <-time.After(100 * time.Millisecond):
					// Timeout - preskoči to naročo
				}
				break
			}
		}
	}
}

// ============================================================================
// Helper functions
// ============================================================================

func generateToken(userID int64) string {
	b := make([]byte, 32)
	rand.Read(b)
	return fmt.Sprintf("%d_%s", userID, hex.EncodeToString(b))
}
