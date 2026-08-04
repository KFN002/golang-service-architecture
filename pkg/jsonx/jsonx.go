// Package jsonx is the project-wide JSON codec.
//
// It fronts bytedance/sonic — a JIT/SIMD-accelerated, drop-in compatible
// encoder that is the fastest production option on amd64 and arm64; sonic
// itself falls back to a reflect-based implementation on other platforms, so
// this package is portable without build tags. All services and the Fiber
// JSON configuration route through here (single choke point, DIP).
package jsonx

import "github.com/bytedance/sonic"

var cfg = sonic.ConfigDefault // std-compatible semantics

// Marshal encodes v as JSON.
func Marshal(v any) ([]byte, error) { return cfg.Marshal(v) }

// Unmarshal decodes data into v.
func Unmarshal(data []byte, v any) error { return cfg.Unmarshal(data, v) }
