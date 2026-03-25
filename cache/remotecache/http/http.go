package http

import (
	"strconv"
	"time"

	"github.com/moby/buildkit/solver"
	"github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
)

const (
	attrEndpointURL       = "endpoint_url"
	attrUploadParallelism = "upload_parallelism"
)

type Config struct {
	EndpointURL       string
	UploadParallelism int
}

func getConfig(attrs map[string]string) (Config, error) {
	endpointURL, ok := attrs[attrEndpointURL]
	if !ok {
		return Config{}, errors.Errorf("endpoint_url not set for http cache")
	}

	uploadParallelism := 4
	uploadParallelismStr, ok := attrs[attrUploadParallelism]
	if ok {
		uploadParallelismInt, err := strconv.Atoi(uploadParallelismStr)
		if err != nil {
			return Config{}, errors.Errorf("upload_parallelism must be a positive integer")
		}
		if uploadParallelismInt <= 0 {
			return Config{}, errors.Errorf("upload_parallelism must be a positive integer")
		}
		uploadParallelism = uploadParallelismInt
	}

	return Config{
		EndpointURL:       endpointURL,
		UploadParallelism: uploadParallelism,
	}, nil
}

type QueryRequest struct {
	Inputs     []CacheKeyWithSelector `json:"inputs,omitempty"`
	Digest     digest.Digest          `json:"digest"`
	InputIndex solver.Index           `json:"input_index"`
}

type QueryResponse struct {
	CacheKeys []string `json:"cache_keys"`
}

type RecordsResponse struct {
	Records []Record `json:"records"`
}

type Record struct {
	ID        string     `json:"id"`
	CreatedAt *time.Time `json:"createdAt,omitempty"`
}

type CacheKeyWithSelector struct {
	CacheKey string        `json:"id"`
	Selector digest.Digest `json:"selector,omitempty"`
}

type CacheKey struct {
	ID   string                   `json:"id"`
	Deps [][]CacheKeyWithSelector `json:"deps,omitempty"`
}
