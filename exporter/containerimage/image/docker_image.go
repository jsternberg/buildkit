package image

import (
	"time"

	"github.com/docker/docker/api/types/strslice"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
)

// HealthConfig holds configuration settings for the HEALTHCHECK feature.
type HealthConfig struct {
	// Test is the test to perform to check that the container is healthy.
	// An empty slice means to inherit the default.
	// The options are:
	// {} : inherit healthcheck
	// {"NONE"} : disable healthcheck
	// {"CMD", args...} : exec arguments directly
	// {"CMD-SHELL", command} : run command with system's default shell
	Test []string `json:",omitempty"`

	// Zero means to inherit. Durations are expressed as integer nanoseconds.
	Interval      time.Duration `json:",omitempty"` // Interval is the time to wait between checks.
	Timeout       time.Duration `json:",omitempty"` // Timeout is the time to wait before considering the check to have hung.
	StartPeriod   time.Duration `json:",omitempty"` // The start period for the container to initialize before the retries starts to count down.
	StartInterval time.Duration `json:",omitempty"` // StartInterval is the time to wait between checks during the start period.

	// Retries is the number of consecutive failures needed to consider a container as unhealthy.
	// Zero means inherit.
	Retries int `json:",omitempty"`
}

// ImageConfig is a docker compatible config for an image
type ImageConfig struct {
	ocispecs.ImageConfig

	Healthcheck *HealthConfig `json:",omitempty"` // Healthcheck describes how to check the container is healthy

	OnBuild []string          `json:",omitempty"` // ONBUILD metadata that were defined on the image Dockerfile
	Shell   strslice.StrSlice `json:",omitempty"` // Shell for shell-form of RUN, CMD, ENTRYPOINT
}

// Image is the JSON structure which describes some basic information about the image.
// This provides the `application/vnd.oci.image.config.v1+json` mediatype when marshalled to JSON.
type Image struct {
	ocispecs.Image

	// Config defines the execution parameters which should be used as a base when running a container using the image.
	Config ImageConfig `json:"config,omitempty"`
}
