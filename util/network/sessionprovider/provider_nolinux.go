//go:build !linux

package sessionprovider

func setupRoutes(_ string) error {
	return nil
}
