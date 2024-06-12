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
	fmt.Printf("input: %+v\n", input)
	for tableName, keys := range input.RequestItems {
		fmt.Printf("tableName: %s keys: %+v\n", tableName, keys)
		for _, key := range keys.Keys {
			fmt.Printf("key: %+v\n", key)
			// unmarshal raw response object to DDB attribute values map
			var t map[string]interface{}
			err := attributevalue.UnmarshalMap(key, &t)
			if err != nil {
				log.Printf("error decoding output item to store in cache err=%+v\n", err)
				return middleware.InitializeOutput{}, nil // dont return err
			}
			fmt.Printf("t: %+v\n", t)
			cacheKey, err := ComputeCacheKey(tableName, key)
			if err != nil {
				panic(err)
			}
			fmt.Printf("cache key: %s\n", cacheKey)
		}
	}

	return middleware.InitializeOutput{}, errors.New("not implemented")
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
		if err != nil {
			return middleware.InitializeOutput{}, fmt.Errorf("error looking up item in cache: %w", err)
		}

		switch r := rsp.(type) {
		case *responses.GetHit:
			// TODO: pull this into a "marshal" function
			// On hit decode value from stored json to DDB attribute map
			var t map[string]interface{}
			err := json.NewDecoder(bytes.NewReader(r.ValueByte())).Decode(&t)
			if err != nil {
				return middleware.InitializeOutput{}, fmt.Errorf("error decoding json item in cache to return: %w", err)
			}

			// Marshal from attribute map to dynamodb.GetItemOutput.Item
			marshalMap, err := attributevalue.MarshalMap(t)
			if err != nil {
				return middleware.InitializeOutput{}, fmt.Errorf("error encoding item in cache to ddbItem to return: %w", err)
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

			// TODO: Pull out all code before the `set` into an `unmarshal()` function
			// unmarshal raw response object to DDB attribute values map
			var t map[string]interface{}
			err := attributevalue.UnmarshalMap(o.Item, &t)
			if err != nil {
				log.Printf("error decoding output item to store in cache err=%+v\n", err)
				return out, nil // dont return err
			}

			// Marshal to JSON to store in cache
			j, err := json.Marshal(t)
			if err != nil {
				log.Printf("error json encoding new item to store in cache err=%+v\n", err)
				return out, nil // dont return err
			}

			// set item in momento cache
			_, err = d.momentoClient.Set(ctx, &momento.SetRequest{
				CacheName: d.cacheName,
				Key:       momento.String(cacheKey),
				Value:     momento.Bytes(j),
			})
			if err != nil {
				log.Printf("error storing item in cache err=%+v\n", err)
				return out, nil // dont return err
			}
		}
	}

	// unsupported output just return output and dont do anything
	return out, err
}

// TODO: pull out to a helper package - rename to something better
func ComputeCacheKey(tableName string, keys map[string]types.AttributeValue) (string, error) {
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

	// prefix JSON key w/ table name and convert to fixed length hash
	hash := sha256.New()
	hash.Write([]byte(tableName + string(out)))
	return hex.EncodeToString(hash.Sum(nil)), nil
}
