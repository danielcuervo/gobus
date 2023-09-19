package kinesis

import (
	"encoding/json"
	"foodienizer.com/libs/bus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type KinesisAsyncPublisher struct {
	session     *session.Session
	connection  *kinesis.Kinesis
	streamNames map[string]bool
}

func NewKinesisAsyncPublisher(region string, endpoint string, accessKeyID string, secretAccessKey string, sessionToken string) KinesisAsyncPublisher {
	s := session.New(&aws.Config{
		Region:   aws.String(region),
		Endpoint: aws.String(endpoint),
		Credentials: credentials.NewStaticCredentials(
			accessKeyID,
			secretAccessKey,
			sessionToken,
		),
	})

	kc := kinesis.New(s)

	finishedListStream := false
	var sn map[string]bool
	for finishedListStream {
		listStreamOutput, err := kc.ListStreams(&kinesis.ListStreamsInput{})
		if err != nil {
			finishedListStream = true
		}

		for _, name := range listStreamOutput.StreamNames {
			sn[*name] = true
		}

		finishedListStream = *listStreamOutput.HasMoreStreams
	}

	return KinesisAsyncPublisher{
		session:     s,
		connection:  kc,
		streamNames: sn,
	}
}

func (k KinesisAsyncPublisher) Publish(message bus.Message) error {
	if !k.streamNames[message.Name()] {
		err := k.createStream(k.connection, message)
		if err != nil {
			return err
		}
	}

	streamName := aws.String(message.Name())
	_, err := k.connection.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})
	if err != nil {
		return err
	}

	jsonMessage, err := json.Marshal(message)
	if err != nil {
		return err
	}

	_, err = k.connection.PutRecord(&kinesis.PutRecordInput{
		Data:         jsonMessage,
		StreamName:   streamName,
		PartitionKey: aws.String("partition1"),
	})
	if err != nil {
		return err
	}

	return nil
}

func (k KinesisAsyncPublisher) createStream(kinesisConnection *kinesis.Kinesis, message bus.Message) error {
	streamName := aws.String(message.Name())
	_, err := kinesisConnection.CreateStream(&kinesis.CreateStreamInput{
		ShardCount: aws.Int64(1),
		StreamName: streamName,
	})
	if err != nil {
		return err
	}

	if err := kinesisConnection.WaitUntilStreamExists(&kinesis.DescribeStreamInput{StreamName: streamName}); err != nil {
		return err
	}

	k.streamNames[message.Name()] = true
	return nil
}

func (k KinesisAsyncPublisher) deleteStream(kinesisConnection *kinesis.Kinesis, message bus.Message) error {
	_, err := kinesisConnection.DeleteStream(&kinesis.DeleteStreamInput{
		StreamName: aws.String(message.Name()),
	})

	if err != nil {
		return err
	}

	return nil
}
