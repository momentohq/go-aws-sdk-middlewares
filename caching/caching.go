package caching

import (
	"bytes"
	"context"
	"encoding/json"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/momentohq/client-sdk-go/momento"
	"github.com/momentohq/client-sdk-go/responses"
)

func AttachNewCachingMiddleware(cfg *aws.Config, cacheName string, client momento.CacheClient) {
	cfg.APIOptions = append(cfg.APIOptions, func(stack *middleware.Stack) error {
		return stack.Initialize.Add(
			NewCachingMiddleware(cacheName, client),
			middleware.Before,
		)
	})
}

func NewCachingMiddleware(cacheName string, momentoClient momento.CacheClient) middleware.InitializeMiddleware {
	return middleware.InitializeMiddlewareFunc("CachingMiddleware", func(
		ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler,
	) (out middleware.InitializeOutput, metadata middleware.Metadata, err error) {

		// check to see if we know how to cache this request type
		switch v := in.Parameters.(type) {
		case *dynamodb.GetItemInput:

			// Derive cache key from DDB request
			keys, err := normalizeKeys(*v.TableName, v.Key)
			if err != nil {
				log.Printf("error getting normalized keys for caching. skipping middleware %+v\n", err)
				return next.HandleInitialize(ctx, in) // continue request execution skip middleware
			}

			// Make sure we don't cache when trying to do a consistent read
			if v.ConsistentRead == nil {

				// Try look up value in momento
				rsp, err := momentoClient.Get(ctx, &momento.GetRequest{
					CacheName: cacheName,
					Key:       momento.String(keys),
				})
				if err != nil {
					log.Printf("error looking up item in cache skipping middleware %+v\n", err)
					return next.HandleInitialize(ctx, in) // continue request execution skip middleware
				}

				switch r := rsp.(type) {
				case *responses.GetHit:
					// On hit decode value from stored json to DDB attribute map
					var t map[string]interface{}
					err := json.NewDecoder(bytes.NewReader(r.ValueByte())).Decode(&t)
					if err != nil {
						log.Printf("error decoding json item in cache to return skipping middleware %+v\n", err)
						return next.HandleInitialize(ctx, in) // continue request execution skip middleware
					}

					// Marshal from attribute map to dynamodb.GetItemOutput.Item
					marshalMap, err := attributevalue.MarshalMap(t)
					if err != nil {
						log.Printf("error encoding item in cache to ddbItem to return skipping middleware %+v\n", err)
						return next.HandleInitialize(ctx, in) // continue request execution skip middleware
					}

					// Return user spoofed dynamodb.GetItemOutput.Item w/ cached value
					return struct{ Result interface{} }{Result: &dynamodb.GetItemOutput{
						Item: marshalMap,
					}}, middleware.Metadata{}, nil

				case *responses.GetMiss:
					// Just log on miss
					log.Printf("Look up did not find a value key=%s\n", keys)
				}
			}

			// On MISS Let middleware chain continue so we can get result and try to cache it
			out, md, err := next.HandleInitialize(ctx, in)

			switch o := out.Result.(type) {
			case *dynamodb.GetItemOutput:
				// unmarshal raw response object to DDB attribute values map
				var t map[string]interface{}
				err := attributevalue.UnmarshalMap(o.Item, &t)
				if err != nil {
					log.Printf("error decoding output item to store in cache err=%+v\n", err)
					return out, md, nil // dont return err
				}

				// Marshal to JSON to store in cache
				j, err := json.Marshal(t)
				if err != nil {
					log.Printf("error json encoding new item to store in cache err=%+v\n", err)
					return out, md, nil // dont return err
				}

				// set item in momento cache
				_, err = momentoClient.Set(ctx, &momento.SetRequest{
					CacheName: cacheName,
					Key:       momento.String(keys),
					Value:     momento.Bytes(j),
				})
				if err != nil {
					log.Printf("error storing item in cache err=%+v\n", err)
					return out, md, nil // dont return err
				}
			}
			// unsupported output just return output and dont do anything
			return out, md, err
		}
		// AWS SDK CMD not supported just continue as normal
		return next.HandleInitialize(ctx, in)
	})
}
func normalizeKeys(tableName string, keys map[string]types.AttributeValue) (string, error) {
	// Marshal to attribute map
	var t map[string]interface{}
	err := attributevalue.UnmarshalMap(keys, &t)
	if err != nil {
		return "", err
	}

	// encode key as json string
	out, err := json.Marshal(t)
	if err != nil {
		return "", err
	}

	// prefix JSON key w/ table name
	return tableName + string(out), nil
}
