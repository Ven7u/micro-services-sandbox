package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Review structure
type Review struct {
	ReviewID string  `json:"review_id"`
	MovieID  string  `json:"movie_id"`
	UserID   string  `json:"user_id"`
	Title    string  `json:"title"`
	Review   string  `json:"review"`
	Score    float32 `json:"score"`
}

// Movie structure
type Movie struct {
	MovieID      string  `json:"movie_id"`
	Title        string  `json:"title"`
	AverageScore float32 `json:"average_score"`
}

var db *pgxpool.Pool

// Database connection
func connectDB(ctx context.Context) {
	var err error
	db, err = pgxpool.New(ctx, "postgres://app_user:app_password@postgres:5432/app_db")
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	log.Println("Connected to database")
}

func initDB(ctx context.Context) {
	log.Println("Initializing DB")
	_, err := db.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS movies (
		movie_id TEXT PRIMARY KEY,
		title TEXT NOT NULL,
		average_score REAL DEFAULT 0
	);

	CREATE TABLE IF NOT EXISTS reviews (
		review_id TEXT PRIMARY KEY,
		movie_id TEXT NOT NULL,
		user_id TEXT NOT NULL,
		title TEXT NOT NULL,
		review TEXT NOT NULL,
		score REAL NOT NULL
	);
	`)
	if err != nil {
		log.Fatal("Error Initializing DB: %w", err)

	}
}

// Movie Handlers

func upsertMovie(w http.ResponseWriter, r *http.Request) {
	var movie Movie
	if err := json.NewDecoder(r.Body).Decode(&movie); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	log.Printf("Upserting movie %v", movie)

	// Upsert the movie: If the movie_id exists, update it; otherwise, insert a new movie
	query := `
		INSERT INTO movies (movie_id, title, average_score)
		VALUES ($1, $2, $3)
		ON CONFLICT (movie_id) 
		DO UPDATE SET
			title = EXCLUDED.title,
			average_score = EXCLUDED.average_score
		RETURNING movie_id;
	`

	// Assuming average_score and number_of_reviews are part of the request, if not, set them accordingly.
	err := db.QueryRow(
		context.Background(),
		query,
		movie.MovieID, movie.Title, movie.AverageScore,
	).Scan(&movie.MovieID)
	if err != nil {
		http.Error(w, "Failed to upsert movie", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK) // 200 OK, since we're updating or inserting
	json.NewEncoder(w).Encode(movie)
}

func listMovies(w http.ResponseWriter, r *http.Request) {
	log.Printf("Listing movies")

	rows, err := db.Query(context.Background(), "SELECT movie_id, title, average_score FROM movies")
	if err != nil {
		http.Error(w, "Failed to retrieve movies", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var movies []Movie
	for rows.Next() {
		var movie Movie
		if err := rows.Scan(&movie.MovieID, &movie.Title, &movie.AverageScore); err != nil {
			http.Error(w, "Error scanning movie", http.StatusInternalServerError)
			return
		}
		movies = append(movies, movie)
	}

	json.NewEncoder(w).Encode(movies)
}

// Review Handlers

func createReview(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	var review Review
	if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	log.Printf("Creating review %v", review)

	// Insert the review
	err := db.QueryRow(
		ctx,
		"INSERT INTO reviews (review_id, movie_id, user_id, title, review, score) VALUES ($1, $2, $3, $4, $5, $6) RETURNING review_id",
		review.ReviewID, review.MovieID, review.UserID, review.Title, review.Review, review.Score,
	).Scan(&review.ReviewID)
	if err != nil {
		http.Error(w, "Failed to create review", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(review)
}

func listReviews(w http.ResponseWriter, r *http.Request) {
	movieID := r.URL.Query().Get("movie_id")
	var rows pgx.Rows
	var err error

	if movieID == "" {
		log.Printf("Listing reviews for movie %v", movieID)
		rows, err = db.Query(context.Background(), "SELECT review_id, movie_id, user_id, title, review, score FROM reviews")
	} else {
		log.Printf("Listing reviews")
		rows, err = db.Query(context.Background(), "SELECT review_id, movie_id, user_id, title, review, score FROM reviews WHERE movie_id = $1", movieID)
	}

	if err != nil {
		http.Error(w, "Failed to retrieve reviews", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var reviews []Review
	for rows.Next() {
		var review Review
		if err := rows.Scan(&review.ReviewID, &review.MovieID, &review.UserID, &review.Title, &review.Review, &review.Score); err != nil {
			http.Error(w, "Error scanning review", http.StatusInternalServerError)
			return
		}
		reviews = append(reviews, review)
	}

	json.NewEncoder(w).Encode(reviews)
}

// Main function
func main() {
	ctx := context.Background()

	connectDB(ctx)
	defer db.Close()

	initDB(ctx)

	http.HandleFunc("/movies", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			upsertMovie(w, r)
		case http.MethodGet:
			listMovies(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	http.HandleFunc("/reviews", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			createReview(w, r)
		case http.MethodGet:
			listReviews(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	fmt.Println("Server running on port 8081...")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
