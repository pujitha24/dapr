/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package stream

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/dapr/dapr/pkg/placement/internal/loops"
	v1pb "github.com/dapr/dapr/pkg/proto/placement/v1"
	"github.com/dapr/kit/events/loop/fake"
)

// fakeChannel is a minimal Placement_ReportDaprStatusServer whose Send result
// is controlled by the test.
type fakeChannel struct {
	grpc.ServerStream
	sendErr error
}

func (f *fakeChannel) Send(*v1pb.PlacementOrder) error { return f.sendErr }
func (f *fakeChannel) Recv() (*v1pb.Host, error)       { return nil, io.EOF }

// TestHandle_SendFailureCancelsInsteadOfEnqueueing guards against the
// regression described in https://github.com/dapr/dapr/issues/10323: a send
// failure used to enqueue a ConnCloseStream directly, while recvLoop
// separately enqueues one when it unwinds after the stream is cancelled.
// That double delivery underflows the namespace connection counter. Handle
// must only cancel the stream context and leave close reporting to recvLoop.
func TestHandle_SendFailureCancelsInsteadOfEnqueueing(t *testing.T) {
	sendErr := errors.New("send failed")

	var cancelCalls int
	var cancelledWith error
	cancel := func(err error) {
		cancelCalls++
		cancelledWith = err
	}

	var enqueueCalls int
	nsLoop := fake.New[loops.EventNamespace]().WithEnqueue(func(loops.EventNamespace) { enqueueCalls++ })

	s := &stream{
		idx:     1,
		ns:      "default",
		channel: &fakeChannel{sendErr: sendErr},
		cancel:  cancel,
		nsLoop:  nsLoop,
		addr:    "test-addr",
	}

	err := s.Handle(t.Context(), &loops.DisseminateLock{Version: 1})
	require.NoError(t, err)

	assert.Equal(t, 1, cancelCalls, "send failure must cancel the stream context exactly once")
	require.ErrorIs(t, cancelledWith, sendErr, "the send error must be preserved as the cancellation cause")
	assert.Equal(t, 0, enqueueCalls, "Handle must not enqueue ConnCloseStream itself; recvLoop is the single emission point")
}

// TestHandle_SendSuccessDoesNotCancel ensures the happy path is untouched by
// the fix: no cancellation and no close event when the send succeeds.
func TestHandle_SendSuccessDoesNotCancel(t *testing.T) {
	var cancelCalls int
	cancel := func(error) { cancelCalls++ }

	var enqueueCalls int
	nsLoop := fake.New[loops.EventNamespace]().WithEnqueue(func(loops.EventNamespace) { enqueueCalls++ })

	s := &stream{
		idx:     1,
		ns:      "default",
		channel: &fakeChannel{},
		cancel:  cancel,
		nsLoop:  nsLoop,
		addr:    "test-addr",
	}

	err := s.Handle(t.Context(), &loops.DisseminateLock{Version: 1})
	require.NoError(t, err)

	assert.Equal(t, 0, cancelCalls)
	assert.Equal(t, 0, enqueueCalls)
}
