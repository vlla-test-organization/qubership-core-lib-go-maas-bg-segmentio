package blue_green_segmentio

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestAdminManyBrokersLastBrokerAvailable(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	testConn := &kafka.Conn{}
	testUnavailableErr := fmt.Errorf("unavailable")
	adapter := adminAdapter{addr: &testAddr{
		network: "tcp,tcp,tcp",
		address: "test.kafka-1:9092,test.kafka-2:9092,test.kafka-3:9092",
	}, dialer: &testDialer{results: map[string]*connAndErr{
		"tcp@test.kafka-1:9092": {conn: nil, err: testUnavailableErr},
		"tcp@test.kafka-2:9092": {conn: nil, err: testUnavailableErr},
		"tcp@test.kafka-3:9092": {conn: testConn, err: nil},
	}}}
	conn, err := adapter.connectToAnyBroker(ctx)
	assertions.NoError(err)
	assertions.True(reflect.DeepEqual(testConn, conn))
}

func TestAdminManyBrokersLastNoBrokerAvailable(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	testUnavailableErr := fmt.Errorf("unavailable")
	adapter := adminAdapter{addr: &testAddr{
		network: "tcp,tcp,tcp",
		address: "test.kafka-1:9092,test.kafka-2:9092,test.kafka-3:9092",
	}, dialer: &testDialer{results: map[string]*connAndErr{
		"tcp@test.kafka-1:9092": {conn: nil, err: testUnavailableErr},
		"tcp@test.kafka-2:9092": {conn: nil, err: testUnavailableErr},
		"tcp@test.kafka-3:9092": {conn: nil, err: testUnavailableErr},
	}}}
	conn, err := adapter.connectToAnyBroker(ctx)
	assertions.Equal(testUnavailableErr, err)
	assertions.Nil(conn)
}

type testAddr struct {
	network string
	address string
}

func (add *testAddr) Network() string {
	return add.network
}
func (add *testAddr) String() string {
	return add.address
}

type testDialer struct {
	results map[string]*connAndErr
}

type connAndErr struct {
	conn *kafka.Conn
	err  error
}

func (t *testDialer) Dial(network string, address string) (*kafka.Conn, error) {
	cAndE := t.results[fmt.Sprintf("%s@%s", network, address)]
	if cAndE == nil {
		return nil, fmt.Errorf("unknown network/address")
	}
	return cAndE.conn, cAndE.err
}
