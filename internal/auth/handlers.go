package auth

import (
	"context"
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
	"time"

	"github.com/jmoiron/sqlx"
)

// AuthHandler handles authentication requests
type AuthHandler struct {
	userRepo *UserRepository
}

// NewAuthHandler creates a new AuthHandler
func NewAuthHandler(db *sqlx.DB) (*AuthHandler, error) {
	repo, err := NewUserRepository(db)
	if err != nil {
		return nil, err
	}
	return &AuthHandler{userRepo: repo}, nil
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token     string   `json:"token"`
	Username  string   `json:"username"`
	Roles     []string `json:"roles"`
	ExpiresAt int64    `json:"expiresAt"`
}

// Login handles user login
func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	user, err := h.userRepo.Authenticate(ctx, req.Username, req.Password)
	if err != nil {
		http.Error(w, "Invalid credentials", http.StatusUnauthorized)
		return
	}

	// Generate token
	token, err := GenerateToken(*user)
	if err != nil {
		http.Error(w, "Failed to generate token", http.StatusInternalServerError)
		return
	}

	// Return token
	resp := loginResponse{
		Token:     token,
		Username:  user.Username,
		Roles:     user.Roles,
		ExpiresAt: time.Now().Add(24 * time.Hour).Unix(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

type registerRequest struct {
	Username string   `json:"username"`
	Password string   `json:"password"`
	Roles    []string `json:"roles"`
}

// Register handles user registration (admin only)
func (h *AuthHandler) Register(w http.ResponseWriter, r *http.Request) {
	// Check if the request comes from an admin
	if !HasRole(r, "admin") {
		http.Error(w, "Unauthorized", http.StatusForbidden)
		return
	}

	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Create user
	userID, err := h.userRepo.CreateUser(ctx, req.Username, req.Password, req.Roles)
	if err != nil {
		http.Error(w, "Failed to create user", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":       userID,
		"username": req.Username,
		"roles":    req.Roles,
	})
}

// RegisterMuxRoutes registers routes for Gorilla Mux router
func (h *AuthHandler) RegisterRoutes(router *mux.Router) {
	// Public authentication endpoints
	router.HandleFunc("/api/auth/login", h.Login).Methods("POST")

	// Protected endpoints (require admin role)
	registerRoute := router.HandleFunc("/api/auth/register", h.Register).Methods("POST")
	registerRoute.Handler(AuthMiddleware(http.HandlerFunc(h.Register)))

	// User info endpoint
	userInfoRoute := router.HandleFunc("/api/auth/me", h.GetUserInfo).Methods("GET")
	userInfoRoute.Handler(AuthMiddleware(http.HandlerFunc(h.GetUserInfo)))
}

// GetUserInfo returns info about the current authenticated user
func (h *AuthHandler) GetUserInfo(w http.ResponseWriter, r *http.Request) {
	claims, ok := r.Context().Value("user").(*Claims)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	user, err := h.userRepo.GetUserByUsername(ctx, claims.Username)
	if err != nil {
		http.Error(w, "Failed to get user info", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":       user.ID,
		"username": user.Username,
		"roles":    user.Roles,
	})
}
