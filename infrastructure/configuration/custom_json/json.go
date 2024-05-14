package custom_json

import (
	"github.com/json-iterator/go"
	"io"
)

var JsonIter = jsoniter.ConfigDefault

func Unmarshal(data []byte, obj interface{}) error {
	return JsonIter.Unmarshal(data, obj)
}

func Marshal(obj interface{}) ([]byte, error) {
	return JsonIter.Marshal(obj)
}

func Decode(reader io.Reader, obj interface{}) error {
	return JsonIter.NewDecoder(reader).Decode(obj)
}

func Encode(writer io.Writer, obj interface{}) error {
	return JsonIter.NewEncoder(writer).Encode(obj)
}
