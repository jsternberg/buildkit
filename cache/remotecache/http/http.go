package http

import (
	"github.com/moby/buildkit/solver"
	"github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
)

const (
	attrEndpointURL = "endpoint_url"
)

type Config struct {
	EndpointURL string
}

func getConfig(attrs map[string]string) (Config, error) {
	endpointURL, ok := attrs[attrEndpointURL]
	if !ok {
		return Config{}, errors.Errorf("endpoint_url not set for http cache")
	}

	return Config{
		EndpointURL: endpointURL,
	}, nil
}

type QueryRequest struct {
	Inputs      []CacheKeyWithSelector `json:"inputs,omitempty"`
	Digest      digest.Digest          `json:"digest"`
	InputIndex  solver.Index           `json:"input_index"`
	OutputIndex solver.Index           `json:"output_index"`
	CacheKeys   []*CacheKey            `json:"cache_keys,omitempty"`
}

type CacheKeyWithSelector struct {
	CacheKey int           `json:"index"`
	Selector digest.Digest `json:"selector,omitempty"`
}

type CacheKey struct {
	ID   string                   `json:"id"`
	Deps [][]CacheKeyWithSelector `json:"deps,omitempty"`
}
