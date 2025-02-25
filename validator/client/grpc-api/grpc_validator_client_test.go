//go:build !use_beacon_api
// +build !use_beacon_api

package grpc_api

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/prysmaticlabs/prysm/v3/testing/assert"
	mock2 "github.com/prysmaticlabs/prysm/v3/testing/mock"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestWaitForChainStart_StreamSetupFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	beaconNodeValidatorClient := mock2.NewMockBeaconNodeValidatorClient(ctrl)
	beaconNodeValidatorClient.EXPECT().WaitForChainStart(
		gomock.Any(),
		gomock.Any(),
	).Return(nil, errors.New("failed stream"))

	validatorClient := &grpcValidatorClient{beaconNodeValidatorClient}
	_, err := validatorClient.WaitForChainStart(context.Background(), &emptypb.Empty{})
	want := "could not setup beacon chain ChainStart streaming client"
	assert.ErrorContains(t, want, err)
}
