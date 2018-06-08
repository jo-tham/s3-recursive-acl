package main

import (
	"context"
	"fmt"
	"sync"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/ti/nasync"
)

// Handler is your Lambda function handler
func Handler(ctx context.Context, e events.DynamoDBEvent) {
	for _, record := range e.Records {
		log.Printf("Processing request data for event ID %s, type %s.\n", record.EventID, record.EventName)

		// Print new values for attributes name and age
		acl := record.Change.NewImage["acl"].String()
		bucket := record.Change.NewImage["bucket"].String()
		region := record.Change.NewImage["region"].String()
		path := record.Change.NewImage["path"].String()

		// stdout and stderr are sent to AWS CloudWatch Logs
		log.Printf("Processing %s %s %s %s\n", region, bucket, path, acl)

		// var wg sync.WaitGroup
		// var counter int64
		// async := nasync.New(1000,1000)
		// defer async.Close()

		// svc := s3.New(session.New(), &aws.Config{
		// 	Region: aws.String(region),
		// })

		// err := svc.ListObjectsPages(&s3.ListObjectsInput{
		// 	Prefix: aws.String(path),
		// 	Bucket: aws.String(bucket),
		// }, func(page *s3.ListObjectsOutput, lastPage bool) bool {
		// 	for _, object := range page.Contents {
		// 		key := *object.Key
		// 		counter++
		// 		async.Do(func(bucket string, key string, cannedACL string) {
		// 			wg.Add(1)
		// 			_, err := svc.PutObjectAcl(&s3.PutObjectAclInput{
		// 				ACL:    aws.String(cannedACL),
		// 				Bucket: aws.String(bucket),
		// 				Key:    aws.String(key),
		// 			})
		// 			log.Printf(fmt.Sprintf("Updating '%s'", key))
		// 			if err != nil {
		// 				log.Printf(fmt.Sprintf("Failed to change permissions on '%s', %v", key, err))
		// 			}
		// 			defer wg.Done()
		// 		}, bucket, key, acl)
		// 	}
		// 	return true
		// })

		// wg.Wait()

		// if err != nil {
		// 	log.Printf(fmt.Sprintf("Failed to update object permissions in '%s', %v", bucket, err))
		// 	panic(fmt.Sprintf("Failed to update object permissions in '%s', %v", bucket, err))
		// }

		// log.Printf(fmt.Sprintf("Successfully updated permissions on %d objects", counter))
	}
}

func main() {
	lambda.Start(Handler)
}

