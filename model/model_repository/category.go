package model_repository

import "time"

type Category struct {
	Id        int64     `json:"id"`
	Name      string    `json:"name"`
	Version   int16     `json:"version"`
	IndexedAt time.Time `json:"indexedAt"`
}
