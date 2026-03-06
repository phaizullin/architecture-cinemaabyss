package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/segmentio/kafka-go"
)

type EventType string

const (
	MovieEvent   EventType = "movie"
	UserEvent    EventType = "user"
	PaymentEvent EventType = "payment"
)

type MovieEventData struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`
	Rating      *float64 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description string   `json:"description,omitempty"`
}

type UserEventData struct {
	UserID    int       `json:"user_id"`
	Username  *string   `json:"username,omitempty"`
	Email     *string   `json:"email,omitempty"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type PaymentEventData struct {
	PaymentID  int       `json:"payment_id"`
	UserID     int       `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType *string   `json:"method_type,omitempty"`
}

type Event struct {
	ID        string      `json:"id"`
	Type      EventType   `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

type KafkaConfig struct {
	Brokers []string
	Topics  map[EventType]string
}

type EventsService struct {
	kafkaConfig *KafkaConfig
	producer    *kafka.Writer
	consumers   map[EventType]*kafka.Reader
	mu          sync.RWMutex
}

func main() {
	kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka:9092")
	port := getEnv("PORT", "8082")

	kafkaConfig := &KafkaConfig{
		Brokers: []string{kafkaBrokers},
		Topics: map[EventType]string{
			MovieEvent:   "movie-events",
			UserEvent:    "user-events",
			PaymentEvent: "payment-events",
		},
	}

	eventsService := &EventsService{
		kafkaConfig: kafkaConfig,
		consumers:   make(map[EventType]*kafka.Reader),
	}

	eventsService.initProducer()

	eventsService.initConsumers()

	go eventsService.startConsumers()

	router := mux.NewRouter()

	router.HandleFunc("/health", eventsService.healthHandler).Methods("GET")
	router.HandleFunc("/api/events/health", eventsService.healthHandler).Methods("GET")

	router.HandleFunc("/api/events/movie", eventsService.createMovieEvent).Methods("POST")
	router.HandleFunc("/api/events/user", eventsService.createUserEvent).Methods("POST")
	router.HandleFunc("/api/events/payment", eventsService.createPaymentEvent).Methods("POST")

	log.Printf("Starting events service on port %s", port)
	log.Printf("Kafka brokers: %s", kafkaBrokers)

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Println("Shutting down events service...")
		eventsService.shutdown()
		os.Exit(0)
	}()

	log.Fatal(http.ListenAndServe(":"+port, router))
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func (es *EventsService) initProducer() {
	es.producer = &kafka.Writer{
		Addr:     kafka.TCP(es.kafkaConfig.Brokers...),
		Balancer: &kafka.LeastBytes{},
	}
	log.Println("Kafka producer initialized")
}

func (es *EventsService) initConsumers() {
	for eventType, topic := range es.kafkaConfig.Topics {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  es.kafkaConfig.Brokers,
			Topic:    topic,
			GroupID:  fmt.Sprintf("events-service-%s", eventType),
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		})

		es.consumers[eventType] = reader
		log.Printf("Kafka consumer initialized for topic: %s", topic)
	}
}

func (es *EventsService) startConsumers() {
	for eventType, reader := range es.consumers {
		go es.consumeEvents(eventType, reader)
	}
}

func (es *EventsService) consumeEvents(eventType EventType, reader *kafka.Reader) {
	defer reader.Close()

	log.Printf("Starting consumer for %s events", eventType)

	for {
		ctx := context.Background()
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message from %s: %v", eventType, err)
			continue
		}

		var event Event
		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Printf("Error unmarshaling event from %s: %v", eventType, err)
			continue
		}

		log.Printf("[CONSUMER] Received %s event: ID=%s, Timestamp=%s, Payload=%+v",
			eventType, event.ID, event.Timestamp.Format(time.RFC3339), event.Payload)
	}
}

func (es *EventsService) shutdown() {
	if es.producer != nil {
		es.producer.Close()
	}

	for eventType, reader := range es.consumers {
		log.Printf("Closing consumer for %s events", eventType)
		reader.Close()
	}
}

func (es *EventsService) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": true,
	})
}

func (es *EventsService) createMovieEvent(w http.ResponseWriter, r *http.Request) {
	var eventData MovieEventData
	if err := json.NewDecoder(r.Body).Decode(&eventData); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        fmt.Sprintf("movie-%d-%s", eventData.MovieID, eventData.Action),
		Type:      MovieEvent,
		Timestamp: time.Now().UTC(),
		Payload:   eventData,
	}

	partition, offset, err := es.publishEvent(MovieEvent, event)
	if err != nil {
		log.Printf("Error publishing movie event: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	response := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)

	log.Printf("[PRODUCER] Published movie event: ID=%s, Partition=%d, Offset=%d",
		event.ID, partition, offset)
}

func (es *EventsService) createUserEvent(w http.ResponseWriter, r *http.Request) {
	var eventData UserEventData
	if err := json.NewDecoder(r.Body).Decode(&eventData); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        fmt.Sprintf("user-%d-%s", eventData.UserID, eventData.Action),
		Type:      UserEvent,
		Timestamp: time.Now().UTC(),
		Payload:   eventData,
	}

	partition, offset, err := es.publishEvent(UserEvent, event)
	if err != nil {
		log.Printf("Error publishing user event: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	response := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)

	log.Printf("[PRODUCER] Published user event: ID=%s, Partition=%d, Offset=%d",
		event.ID, partition, offset)
}

func (es *EventsService) createPaymentEvent(w http.ResponseWriter, r *http.Request) {
	var eventData PaymentEventData
	if err := json.NewDecoder(r.Body).Decode(&eventData); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        fmt.Sprintf("payment-%d-%s", eventData.PaymentID, eventData.Status),
		Type:      PaymentEvent,
		Timestamp: time.Now().UTC(),
		Payload:   eventData,
	}

	partition, offset, err := es.publishEvent(PaymentEvent, event)
	if err != nil {
		log.Printf("Error publishing payment event: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	response := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)

	log.Printf("[PRODUCER] Published payment event: ID=%s, Partition=%d, Offset=%d",
		event.ID, partition, offset)
}

func (es *EventsService) publishEvent(eventType EventType, event Event) (int, int64, error) {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return 0, 0, err
	}

	topic := es.kafkaConfig.Topics[eventType]
	message := kafka.Message{
		Topic: topic,
		Key:   []byte(event.ID),
		Value: eventJSON,
	}

	err = es.producer.WriteMessages(context.Background(), message)
	if err != nil {
		return 0, 0, err
	}

	return 0, time.Now().UnixNano(), nil
}
