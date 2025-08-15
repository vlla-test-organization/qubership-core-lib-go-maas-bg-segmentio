package blue_green_segmentio

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	blue_green_kafka "github.com/vlla-test-organization/qubership-core-lib-go-bg-kafka/v3"
	bgKafkaTest "github.com/vlla-test-organization/qubership-core-lib-go-bg-kafka/v3/test"
	"github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/classifier"
	"github.com/vlla-test-organization/qubership-core-lib-go-maas-client/v3/kafka/model"
	segmentio "github.com/vlla-test-organization/qubership-core-lib-go-maas-segmentio/v3"
)

func TestBGConsumer(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()

	brokers, err := bgKafkaTest.StartContainers(t)
	assertions.NoError(err)

	topic := "orders"
	topicAddress := model.TopicAddress{
		Classifier:      classifier.New("test"),
		TopicName:       topic,
		NumPartitions:   1,
		BoostrapServers: map[string][]string{"PLAINTEXT": brokers},
	}

	bgKafkaTest.RunBGConsumerTest(t, topic,
		func(options []blue_green_kafka.Option) (*blue_green_kafka.BgConsumer, error) {
			return NewBgConsumer(ctx, topicAddress, "test-prep-go", options...)
		},
		func() (bgKafkaTest.Writer, error) {
			w, err := segmentio.NewWriter(topicAddress)
			if err != nil {
				return nil, err
			}
			return &writer{writer: w}, nil
		},
		func(replicationFactor int) error {
			admin, err := getAdminSupplier(topicAddress)
			client := admin.(*adminAdapter).client
			_, err = client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
				Topics: []kafka.TopicConfig{{
					Topic:             topicAddress.TopicName,
					NumPartitions:     topicAddress.NumPartitions,
					ReplicationFactor: replicationFactor,
				}},
				ValidateOnly: false,
			})
			return err
		})
}

type writer struct {
	writer *kafka.Writer
}

func (w *writer) Write(ctx context.Context, msg blue_green_kafka.Message) error {
	var headers []kafka.Header
	for _, h := range msg.Headers() {
		headers = append(headers, kafka.Header{Key: h.Key, Value: h.Value})
	}
	return w.writer.WriteMessages(ctx, kafka.Message{
		Topic:     msg.Topic(),
		Partition: msg.Partition(),
		Offset:    msg.Offset(),
		Key:       msg.Key(),
		Value:     msg.Value(),
		Headers:   headers,
	})
}
