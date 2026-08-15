package proto

// Tái sinh kiểu Go từ định nghĩa .proto vào proto/gen/go.
//
//	go generate ./proto
//
// Cần protoc và protoc-gen-go trên PATH. Xem README.md.
//go:generate protoc --proto_path=. --go_out=./gen/go --go_opt=paths=source_relative common.proto register.proto heartbeat.proto data.proto error.proto envelope.proto
