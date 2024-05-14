package model_client

type CategoryResponse struct {
	Id      int64  `json:"id"`
	Name    string `json:"name"`
	Version int16  `json:"version"`
}
