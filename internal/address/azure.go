package address

import (
	"context"
	"errors"
)

type azureAssigner struct {
}

func (a *azureAssigner) Assign(_ context.Context, _, _ string, _ []string, _ string) (string, error) {
	return "", nil
}

func (a *azureAssigner) Unassign(_ context.Context, _, _ string) error {
	return nil
}

func (a *azureAssigner) GetIPAddressStats(_ context.Context, _ []string, _ string) (usable, assigned int, err error) {
	return 0, 0, errors.New("GetIPAddressStats not implemented for Azure")
}
