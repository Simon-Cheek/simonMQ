package storage

import "github.com/boltdb/bolt"

type BoltStorage struct {
	db *bolt.DB
}

func NewBoltStorage() (*BoltStorage, error) {
	db, err := bolt.Open("storage.db", 0600, nil)
	if err != nil {
		return nil, err
	}
	return &BoltStorage{db: db}, nil
}
