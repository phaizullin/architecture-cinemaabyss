package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

type Config struct {
	Port                   string
	MonolithURL            string
	MoviesServiceURL       string
	EventsServiceURL       string
	GradualMigration       bool
	MoviesMigrationPercent int
}

type ProxyService struct {
	config     *Config
	httpClient *http.Client
}

func main() {
	config := loadConfig()

	proxy := &ProxyService{
		config: config,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	router := mux.NewRouter()

	router.HandleFunc("/health", proxy.healthHandler).Methods("GET")

	router.HandleFunc("/api/movies", proxy.handleMovies).Methods("GET", "POST")
	router.HandleFunc("/api/movies/{id}", proxy.handleMovieByID).Methods("GET")
	router.HandleFunc("/api/users", proxy.handleUsers).Methods("GET", "POST")
	router.HandleFunc("/api/payments", proxy.handlePayments).Methods("GET", "POST")
	router.HandleFunc("/api/subscriptions", proxy.handleSubscriptions).Methods("GET", "POST")

	log.Printf("Starting proxy service on port %s", config.Port)
	log.Printf("Monolith URL: %s", config.MonolithURL)
	log.Printf("Movies Service URL: %s", config.MoviesServiceURL)
	log.Printf("Events Service URL: %s", config.EventsServiceURL)
	log.Printf("Gradual Migration: %v", config.GradualMigration)
	log.Printf("Movies Migration Percent: %d%%", config.MoviesMigrationPercent)

	log.Fatal(http.ListenAndServe(":"+config.Port, router))
}

func loadConfig() *Config {
	port := getEnv("PORT", "8000")
	monolithURL := getEnv("MONOLITH_URL", "http://monolith:8080")
	moviesServiceURL := getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081")
	eventsServiceURL := getEnv("EVENTS_SERVICE_URL", "http://events-service:8082")

	gradualMigration := getEnv("GRADUAL_MIGRATION", "true") == "true"
	moviesMigrationPercent, _ := strconv.Atoi(getEnv("MOVIES_MIGRATION_PERCENT", "50"))

	return &Config{
		Port:                   port,
		MonolithURL:            monolithURL,
		MoviesServiceURL:       moviesServiceURL,
		EventsServiceURL:       eventsServiceURL,
		GradualMigration:       gradualMigration,
		MoviesMigrationPercent: moviesMigrationPercent,
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func (p *ProxyService) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "healthy",
		"service":   "proxy",
		"timestamp": time.Now().UTC(),
	})
}

func (p *ProxyService) handleMovies(w http.ResponseWriter, r *http.Request) {
	if p.shouldRouteToMicroservice("movies") {
		log.Printf("Routing movies request to microservice")
		p.forwardRequest(w, r, p.config.MoviesServiceURL)
	} else {
		log.Printf("Routing movies request to monolith")
		p.forwardRequest(w, r, p.config.MonolithURL)
	}
}

func (p *ProxyService) handleMovieByID(w http.ResponseWriter, r *http.Request) {
	if p.shouldRouteToMicroservice("movies") {
		log.Printf("Routing movie by ID request to microservice")
		p.forwardRequest(w, r, p.config.MoviesServiceURL)
	} else {
		log.Printf("Routing movie by ID request to monolith")
		p.forwardRequest(w, r, p.config.MonolithURL)
	}
}

func (p *ProxyService) handleUsers(w http.ResponseWriter, r *http.Request) {
	log.Printf("Routing users request to monolith")
	p.forwardRequest(w, r, p.config.MonolithURL)
}

func (p *ProxyService) handlePayments(w http.ResponseWriter, r *http.Request) {
	log.Printf("Routing payments request to monolith")
	p.forwardRequest(w, r, p.config.MonolithURL)
}

func (p *ProxyService) handleSubscriptions(w http.ResponseWriter, r *http.Request) {
	log.Printf("Routing subscriptions request to monolith")
	p.forwardRequest(w, r, p.config.MonolithURL)
}

func (p *ProxyService) shouldRouteToMicroservice(service string) bool {
	if !p.config.GradualMigration {
		return false
	}

	switch service {
	case "movies":
		rand.Seed(time.Now().UnixNano())
		randomPercent := rand.Intn(100) + 1
		return randomPercent <= p.config.MoviesMigrationPercent
	default:
		return false
	}
}

func (p *ProxyService) forwardRequest(w http.ResponseWriter, r *http.Request, targetURL string) {
	target, err := url.Parse(targetURL)
	if err != nil {
		log.Printf("Error parsing target URL: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(target)

	originalPath := r.URL.Path
	r.URL.Host = target.Host
	r.URL.Scheme = target.Scheme
	r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))
	r.Host = target.Host

	log.Printf("Forwarding %s %s to %s", r.Method, originalPath, targetURL)

	proxy.ServeHTTP(w, r)
}
