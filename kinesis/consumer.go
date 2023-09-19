package kinesis

import (
	"context"
	"encoding/json"
	"foodienizer.com/libs/bus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"time"
)

type KinesisAsyncConsumer struct {
	session     *session.Session
	connection  *kinesis.Kinesis
	streamNames map[string]bool
	channel     chan bus.Message
}

func NewKinesisAsyncConsumer(
	region string,
	endpoint string,
	accessKeyID string,
	secretAccessKey string,
	sessionToken string,
) KinesisAsyncConsumer {
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

	return KinesisAsyncConsumer{
		session:     s,
		connection:  kc,
		streamNames: sn,
		channel:     make(chan bus.Message),
	}
}

func (k KinesisAsyncConsumer) Channel() chan bus.Message {
	return k.channel
}

func (k KinesisAsyncConsumer) Consume(message bus.Message, c context.Context) error {
	streamName := aws.String(message.Name())
	streams, err := k.connection.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})
	if err != nil {
		return err
	}

	iteratorOutput, err := k.connection.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:           aws.String(*streams.StreamDescription.Shards[0].ShardId),
		ShardIteratorType: aws.String("LATEST"),
		StreamName:        streamName,
	})
	shardIterator := iteratorOutput.ShardIterator

	var a *string
	for {
		records, err := k.connection.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		})

		if err != nil {
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		if len(records.Records) > 0 {
			for _, d := range records.Records {
				err := json.Unmarshal(d.Data, &message)
				if err != nil {
					continue
				}
				k.channel <- message
			}
		} else if records.NextShardIterator == a || shardIterator == records.NextShardIterator || err != nil {
			iteratorOutput, _ := k.connection.GetShardIterator(&kinesis.GetShardIteratorInput{
				ShardId:           aws.String(*streams.StreamDescription.Shards[0].ShardId),
				ShardIteratorType: aws.String("LATEST"),
				StreamName:        streamName,
			})
			shardIterator = iteratorOutput.ShardIterator
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		shardIterator = records.NextShardIterator
		time.Sleep(1000 * time.Millisecond)
	}

	return nil
}
