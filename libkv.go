package flightKv

import (
	"fmt"
	"sort"
	"strings"

	"github.com/fengleng/flightKv/store"
)

// Initialize creates a new Store object, initializing the client
type Initialize func(addrList []string, options *store.Config) (store.FlightKv, error)

var (
	// Backend initializers
	initializers = make(map[store.KVKind]Initialize)

	supportedBackend = func() string {
		keys := make([]string, 0, len(initializers))
		for k := range initializers {
			keys = append(keys, k.String())
		}
		sort.Strings(keys)
		return strings.Join(keys, ", ")
	}()
)

// NewStore creates an instance of store
func NewStore(kv store.KVKind, addrList []string, options *store.Config) (store.FlightKv, error) {
	if init, exists := initializers[kv]; exists {
		return init(addrList, options)
	}

	return nil, fmt.Errorf("%s %s", store.ErrBackendNotSupported.Error(), supportedBackend)
}

// AddStore adds a new store backend to libkv
func AddStore(kv store.KVKind, init Initialize) {
	initializers[kv] = init
}
