package repositories

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/zeerodex/goot/internal/models"
)

var ErrSnapshotNotFound = errors.New("snapshot not found")

type SnapshotsRepository interface {
	CreateSnapshotForAPI(apiName string, tasks []string) error
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
	query := `INSERT INTO snapshots (api, timestamp, api_ids) VALUES (?, ?, ?)`

	stmt, err := r.db.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare create task statement: %w", err)
	}
	defer stmt.Close()

	jsonIds, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("error marshaling json: %w", err)
	}

	_, err = stmt.Exec(
		apiName,
		time.Now().UTC().Format(time.RFC3339),
		string(jsonIds),
	)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	return nil
}

//
// func (r *snapshotsRepo) GetLastSnapshot() (tasks.APITasks, error) {
// 	query := `SELECT api, timestamp, data FROM snapshots`
//
// 	rows, err := r.db.Query(query)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to query all tasks: %w", err)
// 	}
// 	defer rows.Close()
//
// 	var snapshot models.Snapshot
// 	for rows.Next() {
// 		task, err := r.scanAPITask(rows)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to scan task: %w", err)
// 		}
// 		tasksList = append(tasksList, *task)
// 	}
//
// 	if err := rows.Err(); err != nil {
// 		return nil, fmt.Errorf("error iterating task rows: %w", err)
// 	}
//
// 	return tasksList, nil
// }

func (r *snapshotsRepository) GetLastSnapshot(apiName string) (*models.Snapshot, error) {
	query := `SELECT api, timestamp, api_ids FROM snapshots WHERE api = ? ORDER BY timestamp`

	row := r.db.QueryRow(query, apiName)
	snapshot, err := r.scanSnapshot(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrSnapshotNotFound
		}
		return nil, fmt.Errorf("failed to get last snapshot: %w", err)
	}

	return snapshot, nil
}

// func (r *snapshotsRepo) UpdateTask(task *tasks.APITask) (*tasks.APITask, error) {
// 	if task == nil {
// 		return nil, errors.New("task cannot be nil")
// 	}
// 	if task.APIID == "" {
// 		return nil, errors.New("task API ID cannot be empty")
// 	}
//
// 	query := fmt.Sprintf(`
// 		UPDATE %s
// 		SET title = ?, description = ?, due = ?, completed = ?, last_modified = ?
// 		WHERE api_id = ?`, r.tableName)
//
// 	stmt, err := r.db.Prepare(query)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to prepare update task statement: %w", err)
// 	}
// 	defer stmt.Close()
//
// 	now := time.Now().UTC()
// 	result, err := stmt.Exec(
// 		task.Title,
// 		task.Description,
// 		task.Due.Format(time.RFC3339),
// 		task.Completed,
// 		now.Format(time.RFC3339),
// 		task.APIID,
// 	)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to update task '%s': %w", task.Title, err)
// 	}
//
// 	rowsAffected, err := result.RowsAffected()
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to get affected rows count: %w", err)
// 	}
// 	if rowsAffected == 0 {
// 		return nil, ErrTaskNotFound
// 	}
//
// 	return task, nil
// }
//
// func (r *snapshotsRepo) DeleteTaskByID(id string) error {
// 	if id == "" {
// 		return errors.New("task ID cannot be empty")
// 	}
//
// 	query := fmt.Sprintf("DELETE FROM %s WHERE id = ?", r.tableName)
//
// 	stmt, err := r.db.Prepare(query)
// 	if err != nil {
// 		return fmt.Errorf("failed to prepare delete task statement: %w", err)
// 	}
// 	defer stmt.Close()
//
// 	result, err := stmt.Exec(id)
// 	if err != nil {
// 		return fmt.Errorf("failed to delete task with ID '%s': %w", id, err)
// 	}
//
// 	rowsAffected, err := result.RowsAffected()
// 	if err != nil {
// 		return fmt.Errorf("failed to get affected rows count: %w", err)
// 	}
// 	if rowsAffected == 0 {
// 		return ErrTaskNotFound
// 	}
//
// 	return nil
// }

func (r *snapshotsRepository) scanSnapshot(rows scanner) (*models.Snapshot, error) {
	var snapshot models.Snapshot

	var idsStr, timestamp string
	err := rows.Scan(
		&snapshot.API,
		&timestamp,
		&idsStr,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrSnapshotNotFound
		}
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	snapshot.Timestamp, err = time.Parse(time.RFC3339, timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	var ids []string
	err = json.Unmarshal([]byte(idsStr), &ids)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %w", err)
	}
	snapshot.IDs = ids

	return &snapshot, nil
}

// func (r *snapshotsRepo) scanAPITask(rows scanner) (*tasks.APITask, error) {
// 	var task tasks.APITask
// 	var dueStr, lastModifiedStr string
//
// 	err := rows.Scan(
// 		&task.APIID,
// 		&task.Title,
// 		&task.Description,
// 		&dueStr,
// 		&task.Completed,
// 		&lastModifiedStr,
// 	)
// 	if err != nil {
// 		if errors.Is(err, sql.ErrNoRows) {
// 			return nil, ErrTaskNotFound
// 		}
// 		return nil, fmt.Errorf("failed to scan task row: %w", err)
// 	}
// 	if err := task.SetDueAndLastModified(dueStr, lastModifiedStr); err != nil {
// 		return nil, fmt.Errorf("failed to parse timestamps: %w", err)
// 	}
//
// 	return &task, nil
// }
