package repositories

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/zeerodex/goot/internal/models"
)

type SnapshotsRepository interface {
	CreateSnapshotForAPI(apiName string, ids []string) error
	GetLastSnapshot(apiName string) (*models.Snapshot, error)
}

type snapshotsRepository struct {
	db *sql.DB
}

func NewAPISnapshotsRepository(db *sql.DB) SnapshotsRepository {
	return &snapshotsRepository{
		db: db,
	}
}

func (r *snapshotsRepository) CreateSnapshotForAPI(apiName string, ids []string) error {
	if apiName == "" {
		return errors.New("API name cannot be empty")
	}

	jsonIds, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("failed to marshal IDs: %w", err)
	}

	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec("DELETE FROM snapshots WHERE api = ?", apiName)
	if err != nil {
		return fmt.Errorf("failed to delete existing snapshots: %w", err)
	}

	_, err = tx.Exec(
		"INSERT INTO snapshots (api, timestamp, api_ids) VALUES (?, ?, ?)",
		apiName,
		time.Now().UTC().Format(time.RFC3339),
		string(jsonIds),
	)
	if err != nil {
		return fmt.Errorf("failed to insert snapshot: %w", err)
	}

	return tx.Commit()
}

func (r *snapshotsRepository) GetLastSnapshot(apiName string) (*models.Snapshot, error) {
	if apiName == "" {
		return nil, errors.New("API name cannot be empty")
	}

	query := `SELECT api, timestamp, api_ids FROM snapshots WHERE api = ? ORDER BY timestamp DESC LIMIT 1`
	row := r.db.QueryRow(query, apiName)

	var snapshot models.Snapshot
	var idsStr, timestamp string

	err := row.Scan(&snapshot.API, &timestamp, &idsStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.New("snapshot not found")
		}
		return nil, fmt.Errorf("failed to scan snapshot: %w", err)
	}

	snapshot.Timestamp, err = time.Parse(time.RFC3339, timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	err = json.Unmarshal([]byte(idsStr), &snapshot.IDs)
	if err != nil {
		return nil, fmt.Errorf("failed to parse IDs: %w", err)
	}

	return &snapshot, nil
}
