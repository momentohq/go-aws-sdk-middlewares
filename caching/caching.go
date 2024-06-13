package caching

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/momentohq/client-sdk-go/momento"
	"github.com/momentohq/client-sdk-go/responses"
)

type cachingMiddleware struct {
	cacheName     string
	momentoClient momento.CacheClient
}

func AttachNewCachingMiddleware(cfg *aws.Config, cacheName string, client momento.CacheClient) {
	cfg.APIOptions = append(cfg.APIOptions, func(stack *middleware.Stack) error {
		return stack.Initialize.Add(
			NewCachingMiddleware(&cachingMiddleware{
				cacheName:     cacheName,
				momentoClient: client,
			}),
			middleware.Before,
		)
	})
}

func NewCachingMiddleware(mw *cachingMiddleware) middleware.InitializeMiddleware {
	return middleware.InitializeMiddlewareFunc("CachingMiddleware", func(
		ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler,
	) (out middleware.InitializeOutput, metadata middleware.Metadata, err error) {
		// check to see if we know how to cache this request type
		switch v := in.Parameters.(type) {
		case *dynamodb.GetItemInput:
			o, err := mw.handleGetItemCommand(ctx, v, in, next)
			if err != nil {
				log.Printf("error occurred trying to use caching middleware. skipping middleware %+v\n", err)
				return next.HandleInitialize(ctx, in) // continue request execution skip middleware
			}
			// Middleware ran successfully return our cached output
			return o, middleware.Metadata{}, nil
		case *dynamodb.BatchGetItemInput:
			o, err := mw.handleBatchGetItemCommand(ctx, v, in, next)
			if err != nil {
				log.Printf("error occurred trying to use caching middleware. skipping middleware %+v\n", err)
				return next.HandleInitialize(ctx, in) // continue request execution skip middleware
			}
			// Middleware ran successfully return our cached output
			return o, middleware.Metadata{}, nil
		}
		// AWS SDK CMD not supported just continue as normal
		return next.HandleInitialize(ctx, in)
	})
}

func (d *cachingMiddleware) handleBatchGetItemCommand(ctx context.Context, input *dynamodb.BatchGetItemInput, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, error) {

	responsesToReturn := make(map[string][]map[string]types.AttributeValue)
	gotMiss := false

	// Loop through all tables and keys in the request
	for tableName, keys := range input.RequestItems {
		if responsesToReturn[tableName] == nil {
			responsesToReturn[tableName] = []map[string]types.AttributeValue{}
		}
		for _, key := range keys.Keys {
			fmt.Printf("computing GET key from table: %s and key: %v\n", tableName, key)
			cacheKey, err := ComputeCacheKey(tableName, key)
			// TODO: do we really want to do this? This means users can never access data if our service is down.
			//  Seems like we should just consider this a miss and move on.
			if err != nil {
				return middleware.InitializeOutput{}, fmt.Errorf("error getting key for caching: %w", err)
			}
			fmt.Printf("cacheKey: %s\n", cacheKey)
			rsp, err := d.momentoClient.Get(ctx, &momento.GetRequest{
				CacheName: d.cacheName,
				Key:       momento.String(cacheKey),
			})
			// TODO: Same question as above. This should probably just be a miss?
			if err != nil {
				return middleware.InitializeOutput{}, fmt.Errorf("error looking up item in cache: %w", err)
			}

			switch r := rsp.(type) {
			case *responses.GetHit:
				marshalMap, err := GetMarshalMap(r)
				if err != nil {
					return middleware.InitializeOutput{}, fmt.Errorf("error with martial map: %w", err)
				}
				responsesToReturn[tableName] = append(responsesToReturn[tableName], marshalMap)
			case *responses.GetMiss:
				gotMiss = true
				fmt.Println("miss")
			default:
				return middleware.InitializeOutput{}, fmt.Errorf("unknown type for momento.GetResponse: %T", r)
			}
			if gotMiss {
				break
			}
		}
		if gotMiss {
			break
		}
	}

	if !gotMiss {
		return middleware.InitializeOutput{
			Result: &dynamodb.BatchGetItemOutput{
				Responses: responsesToReturn,
			},
		}, nil
	}

	// On MISS Let middleware chains continue, so we can get result and try to cache it
	out, _, err := next.HandleInitialize(ctx, in)

	if err == nil {
		switch o := out.Result.(type) {
		case *dynamodb.BatchGetItemOutput:
			for tableName, items := range o.Responses {
				for _, item := range items {
					j, err := MarshalToJson(&dynamodb.GetItemOutput{
						Item: item,
					})
					if err != nil {
						return out, err // don't return error
					}

					fmt.Printf("computing SET key from table: %s and key: %v\n", tableName, item)

					cacheKey, err := ComputeCacheKey(tableName, item)
					// set item in momento cache
					_, err = d.momentoClient.Set(ctx, &momento.SetRequest{
						CacheName: d.cacheName,
						Key:       momento.String(cacheKey),
						Value:     momento.Bytes(j),
					})
					if err != nil {
						log.Printf("error storing item in cache err=%+v\n", err)
						return out, nil // don't return err
					}
				}
			}
		}
	}

	// unsupported output just return output and dont do anything
	return out, err
}

func (d *cachingMiddleware) handleGetItemCommand(ctx context.Context, input *dynamodb.GetItemInput, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, error) {

	// Derive a cache key from DDB request
	if input.TableName == nil {
		return middleware.InitializeOutput{}, errors.New("error table name not set on get-item request")
	}
	cacheKey, err := ComputeCacheKey(*input.TableName, input.Key)
	if err != nil {
		return middleware.InitializeOutput{}, fmt.Errorf("error getting key for caching: %w", err)
	}

	// Make sure we don't cache when trying to do a consistent read
	if input.ConsistentRead == nil {
		// Try to look up value in momento
		rsp, err := d.momentoClient.Get(ctx, &momento.GetRequest{
			CacheName: d.cacheName,
			Key:       momento.String(cacheKey),
		})
		// TODO: do we really want to do this? This means users can never access data if our service is down.
		//  Seems like we should just consider this a miss and move on.
		if err != nil {
			return middleware.InitializeOutput{}, fmt.Errorf("error looking up item in cache: %w", err)
		}

		switch r := rsp.(type) {
		case *responses.GetHit:
			// On hit decode value from stored json to DDB attribute map
			marshalMap, err := GetMarshalMap(r)
			if err != nil {
				return middleware.InitializeOutput{}, fmt.Errorf("error with martial map: %w", err)
			}
			// Return user spoofed dynamodb.GetItemOutput.Item w/ cached value
			return struct{ Result interface{} }{Result: &dynamodb.GetItemOutput{
				Item: marshalMap,
			}}, nil

		case *responses.GetMiss:
			// Just log on miss
			log.Printf("Momento lookup did not find key=%s\n", cacheKey)
		}
	}

	// On MISS Let middleware chains continue, so we can get result and try to cache it
	out, _, err := next.HandleInitialize(ctx, in)

	if err == nil {
		switch o := out.Result.(type) {
		case *dynamodb.GetItemOutput:

			// unmarshal raw response object to DDB attribute values map and encode as json
			j, err := MarshalToJson(o)
			if err != nil {
				return out, err // don't return error
			}

			// set item in momento cache
			_, err = d.momentoClient.Set(ctx, &momento.SetRequest{
				CacheName: d.cacheName,
				Key:       momento.String(cacheKey),
				Value:     momento.Bytes(j),
			})
			if err != nil {
				log.Printf("error storing item in cache err=%+v\n", err)
				return out, nil // don't return err
			}
		}
	}

	// unsupported output just return output and dont do anything
	return out, err
}

func ComputeCacheKey(tableName string, keys map[string]types.AttributeValue) (string, error) {
	// Marshal to attribute map
	var t map[string]interface{}
	err := attributevalue.UnmarshalMap(keys, &t)
	if err != nil {
		return "", err
	}

	var keyNames []string
	for k := range t {
		keyNames = append(keyNames, k)
	}
	fmt.Printf("keyNames: %v\n", keyNames)

	// encode key as json string
	out, err := json.Marshal(t)
	if err != nil {
		return "", err
	}

	fmt.Printf("json key: %s\n", string(out))

	// prefix JSON key w/ table name and convert to fixed length hash
	hash := sha256.New()
	hash.Write([]byte(tableName + string(out)))
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func GetMarshalMap(r *responses.GetHit) (map[string]types.AttributeValue, error) {
	// On hit decode value from stored json to DDB attribute map
	var t map[string]interface{}
	err := json.NewDecoder(bytes.NewReader(r.ValueByte())).Decode(&t)
	if err != nil {
		return nil, fmt.Errorf("error decoding json item in cache to return: %w", err)
	}

	// Marshal from attribute map to dynamodb.GetItemOutput.Item
	marshalMap, err := attributevalue.MarshalMap(t)
	if err != nil {
		return nil, fmt.Errorf("error encoding item in cache to ddbItem to return: %w", err)
	}
	return marshalMap, nil
}

func MarshalToJson(item *dynamodb.GetItemOutput) ([]byte, error) {
	// unmarshal raw response object to DDB attribute values map
	var t map[string]interface{}
	err := attributevalue.UnmarshalMap(item.Item, &t)
	if err != nil {
		log.Printf("error decoding output item to store in cache err=%+v\n", err)
		return nil, fmt.Errorf("error decoding output item to store in cache err=%+v\n", err)
	}

	// Marshal to JSON to store in cache
	j, err := json.Marshal(t)
	if err != nil {
		log.Printf("error json encoding new item to store in cache err=%+v\n", err)
		return nil, fmt.Errorf("error json encoding new item to store in cache err=%+v\n", err)
	}
	return j, nil
}
