package auth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
	"golang.org/x/crypto/bcrypt"
)

const userSchema = `
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_roles (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role VARCHAR(50) NOT NULL,
    UNIQUE(user_id, role)
);

CREATE INDEX IF NOT EXISTS idx_user_roles_user_id ON user_roles(user_id);
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
`

// UserRepository handles database operations for users
type UserRepository struct {
	db *sqlx.DB
}

// NewUserRepository creates a new UserRepository
func NewUserRepository(db *sqlx.DB) (*UserRepository, error) {
	// Initialize the schema
	_, err := db.Exec(userSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize user schema: %w", err)
	}
	return &UserRepository{db: db}, nil
}

// CreateUser creates a new user with roles
func (r *UserRepository) CreateUser(ctx context.Context, username, password string, roles []string) (int, error) {
	// Hash the password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return 0, fmt.Errorf("failed to hash password: %w", err)
	}

	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Insert user
	var userID int
	err = tx.QueryRowxContext(ctx,
		"INSERT INTO users (username, password) VALUES ($1, $2) RETURNING id",
		username, string(hashedPassword)).Scan(&userID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert user: %w", err)
	}

	// Insert roles
	for _, role := range roles {
		_, err = tx.ExecContext(ctx,
			"INSERT INTO user_roles (user_id, role) VALUES ($1, $2)",
			userID, role)
		if err != nil {
			return 0, fmt.Errorf("failed to insert role: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return userID, nil
}

// GetUserByUsername retrieves a user by username
func (r *UserRepository) GetUserByUsername(ctx context.Context, username string) (*User, error) {
	var dbUser struct {
		ID       int    `db:"id"`
		Username string `db:"username"`
		Password string `db:"password"`
	}

	err := r.db.GetContext(ctx, &dbUser,
		"SELECT id, username, password FROM users WHERE username = $1", username)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // User not found
		}
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	// Get roles
	var roles []string
	err = r.db.SelectContext(ctx, &roles,
		"SELECT role FROM user_roles WHERE user_id = $1", dbUser.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get user roles: %w", err)
	}

	return &User{
		ID:       dbUser.ID,
		Username: dbUser.Username,
		Password: dbUser.Password,
		Roles:    roles,
	}, nil
}

// Authenticate verifies username and password
func (r *UserRepository) Authenticate(ctx context.Context, username, password string) (*User, error) {
	user, err := r.GetUserByUsername(ctx, username)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, errors.New("user not found")
	}

	// Check password
	err = bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(password))
	if err != nil {
		return nil, errors.New("invalid password")
	}

	return user, nil
}
