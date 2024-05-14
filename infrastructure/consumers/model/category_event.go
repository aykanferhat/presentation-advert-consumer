package model

type CategoryEvent struct {
	Id      int64  `json:"id"`
	Type    string `json:"type"`
	Version int16  `json:"version"`
}
