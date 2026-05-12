// Package rpc implements a JSON codec for gRPC, replacing protobuf wire
// format.  This lets us ship plain Go structs without a protoc build step
// while keeping the full gRPC transport (HTTP/2, streaming, deadlines).
package rpc

import (
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/encoding"
)

const Name = "json"

func init() {
	encoding.RegisterCodec(JSONCodec{})
}

type JSONCodec struct{}

func (JSONCodec) Name() string { return Name }

func (JSONCodec) Marshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("json marshal: %w", err)
	}
	return b, nil
}

func (JSONCodec) Unmarshal(data []byte, v any) error {
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("json unmarshal: %w", err)
	}
	return nil
}
