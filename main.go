package main

import (
	"context"
	"fmt"
	"sync"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/ti/nasync"
)

// Handler is your Lambda function handler
func Handler(ctx context.Context, e events.DynamoDBEvent) {
	c := make(chan struct{})
	timeout := 250 * time.Second
	log.Printf("Wait for handler (up to %s)", timeout)

	go func() {
		for _, record := range e.Records {
			log.Printf("Processing request data for event ID %s, type %s.\n", record.EventID, record.EventName)
			if record.EventName == "REMOVE" {
				log.Print("Operation was REMOVE, exiting")
				defer close(c)
				return
			}
			acl := record.Change.NewImage["acl"].String()
			bucket := record.Change.NewImage["bucket"].String()
			region := record.Change.NewImage["region"].String()
			path := record.Change.NewImage["path"].String()

			// stdout and stderr are sent to AWS CloudWatch Logs
			log.Printf("Processing %s %s %s %s\n", region, bucket, path, acl)

			var wg sync.WaitGroup
			var counter int64
			var hasError bool
			async := nasync.New(1000,1000)
			defer async.Close()
			defer close(c)

			svc := s3.New(session.New(), &aws.Config{
				Region: aws.String(region),
			})

			err := svc.ListObjectsPages(&s3.ListObjectsInput{
				Prefix: aws.String(path),
				Bucket: aws.String(bucket),
			}, func(page *s3.ListObjectsOutput, lastPage bool) bool {
				for _, object := range page.Contents {
					key := *object.Key
					counter++
					wg.Add(1)

					async.Do(func(bucket string, key string, cannedACL string) {
						_, err := svc.PutObjectAcl(&s3.PutObjectAclInput{
							ACL:    aws.String(cannedACL),
							Bucket: aws.String(bucket),
							Key:    aws.String(key),
						})
						log.Printf(fmt.Sprintf("Updating '%s'", key))
						if err != nil {
							log.Printf(fmt.Sprintf("Failed to change permissions on '%s', %v", key, err))
							hasError = true
						}
						defer wg.Done()
					}, bucket, key, acl)
				}
				return true
			})

			wg.Wait()
			if err != nil {
				log.Printf(fmt.Sprintf("Failed to update object permissions in '%s', %v", bucket, err))
				panic(fmt.Sprintf("Failed to update object permissions in '%s', %v", bucket, err))
			}

			if hasError {
				log.Printf("There was an error updating objects at %s, not deleting dynamodb item", path)
				return
			}

			log.Printf(fmt.Sprintf("Successfully updated permissions on %d objects", counter))
			sess, _ := session.NewSession( &aws.Config{
				Region: aws.String("us-west-2")},
			)
			dbsvc := dynamodb.New(sess)
			input := &dynamodb.DeleteItemInput{
				Key: map[string]*dynamodb.AttributeValue{
					"path": {
						S: aws.String(path),
					},
				},
				TableName: aws.String("set-s3-objects-acl"),
			}
			_, err = dbsvc.DeleteItem(input)
			if err != nil {
				log.Printf("Got error calling DeleteItem")
				log.Printf(err.Error())
				return
			}
			log.Printf("Deleted dynamodb record for %s %s %s", region, bucket, path)

		}
		log.Printf("Handler finished")
		// could defer close c here...
	}()

	select {
	case <-time.After(timeout):
		log.Printf("Handler timed out")

	case <-c:
		log.Printf("Handler completed before timeout")
	}
}

func main() {
	lambda.Start(Handler)
}


