package caching

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/momentohq/client-sdk-go/config/logger"
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
				mw.momentoClient.Logger().Warn(
					fmt.Sprintf("error occurred trying to use caching middleware. skipping middleware %+v", err),
				)
				return next.HandleInitialize(ctx, in) // continue request execution skip middleware
			}
			// Middleware ran successfully return our cached output
			return o, middleware.Metadata{}, nil
		case *dynamodb.BatchGetItemInput:
			o, err := mw.handleBatchGetItemCommand(ctx, v, in, next)
			if err != nil {
				mw.momentoClient.Logger().Warn(
					fmt.Sprintf("error occurred trying to use caching middleware. skipping middleware %+v", err),
				)
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
	// for now any cache miss is considered a miss for the whole batch
	gotMiss := false
	// this holds the query keys for each DDB table in the request and is used to compute the cache key for
	// cache set operations
	tableToDdbKeys := make(map[string][]string)
	// this holds the computed Momento cache keys for each item in the request, per table
	tableToCacheKeys := make(map[string][]momento.Key)

	// gather cache keys for batch get from Momento cache
	for tableName, keys := range input.RequestItems {
		// TODO: it may be preferable/safer to query the table for the keys
		gatherKeys := false
		if tableToDdbKeys[tableName] == nil {
			tableToDdbKeys[tableName] = []string{}
			gatherKeys = true
		}
		if responsesToReturn[tableName] == nil {
			responsesToReturn[tableName] = []map[string]types.AttributeValue{}
		}
		if tableToCacheKeys[tableName] == nil {
			tableToCacheKeys[tableName] = []momento.Key{}
		}
		for _, key := range keys.Keys {
			if gatherKeys {
				for keyName := range key {
					tableToDdbKeys[tableName] = append(tableToDdbKeys[tableName], keyName)
				}
				gatherKeys = false
			}
			cacheKey, err := ComputeCacheKey(tableName, key)
			d.momentoClient.Logger().Debug("computed cache key for batch get retrieval: %s", cacheKey)
			if err != nil {
				return middleware.InitializeOutput{}, fmt.Errorf("error getting key for caching: %w", err)
			}
			tableToCacheKeys[tableName] = append(tableToCacheKeys[tableName], momento.String(cacheKey))
		}
	}

	// Batch get from Momento cache and gather response data
	for tableName := range tableToCacheKeys {
		getResp, err := d.momentoClient.GetBatch(ctx, &momento.GetBatchRequest{
			CacheName: d.cacheName,
			Keys:      tableToCacheKeys[tableName],
		})
		if err != nil {
			return middleware.InitializeOutput{}, fmt.Errorf("error looking up item batch in cache: %w", err)
		}
		switch r := getResp.(type) {
		// TODO: fix this in the SDK? Should be returning a reference, but would be a breaking change :-(
		case responses.GetBatchSuccess:
			for _, element := range r.Results() {
				switch e := element.(type) {
				case *responses.GetHit:
					marshalMap, err := GetMarshalMap(e)
					if err != nil {
						return middleware.InitializeOutput{}, fmt.Errorf("error with marshal map: %w", err)
					}
					responsesToReturn[tableName] = append(responsesToReturn[tableName], marshalMap)
				case *responses.GetMiss:
					gotMiss = true
				}
				if gotMiss {
					break
				}
			}
		}
	}

	// If we didn't get a miss, return the responses
	if !gotMiss {
		d.momentoClient.Logger().Debug("returning cached batch get responses")
		return middleware.InitializeOutput{
			Result: &dynamodb.BatchGetItemOutput{
				Responses: responsesToReturn,
			},
		}, nil
	}

	d.momentoClient.Logger().Debug("returning DynamoDB response")
	// On MISS Let middleware chains continue, so we can get result and try to cache it
	out, _, err := next.HandleInitialize(ctx, in)

	if err == nil {
		switch o := out.Result.(type) {
		case *dynamodb.BatchGetItemOutput:
			var itemsToSet []momento.BatchSetItem
			// compute and gather keys and JSON encoded items to store in Momento cache
			for tableName, items := range o.Responses {
				for _, item := range items {
					j, err := MarshalToJson(item, d.momentoClient.Logger())
					if err != nil {
						return out, err // don't return error
					}

					// extract the keys from the item to compute the hash key
					itemForKey := map[string]types.AttributeValue{}
					for _, key := range tableToDdbKeys[tableName] {
						itemForKey[key] = item[key]
					}
					cacheKey, err := ComputeCacheKey(tableName, itemForKey)
					d.momentoClient.Logger().Debug("computed cache key for batch get storage: %s", cacheKey)
					if err != nil {
						return middleware.InitializeOutput{}, fmt.Errorf("error getting key for caching: %w", err)
					}
					itemsToSet = append(itemsToSet, momento.BatchSetItem{
						Key:   momento.String(cacheKey),
						Value: momento.Bytes(j),
					})
				}
			}
			// set item batch in Momento cache
			_, err = d.momentoClient.SetBatch(ctx, &momento.SetBatchRequest{
				CacheName: d.cacheName,
				Items:     itemsToSet,
			})
			if err != nil {
				d.momentoClient.Logger().Warn(
					fmt.Sprintf("error storing item batch in cache err=%+v", err),
				)
				return out, nil // don't return err
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
	d.momentoClient.Logger().Debug("computed cache key for item retrieval: %s", cacheKey)

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
			// On hit decode value from stored json to DDB attribute map
			marshalMap, err := GetMarshalMap(r)
			if err != nil {
				return middleware.InitializeOutput{}, fmt.Errorf("error with marshal map: %w", err)
			}
			d.momentoClient.Logger().Debug("returning cached item")
			// Return user spoofed dynamodb.GetItemOutput.Item w/ cached value
			return struct{ Result interface{} }{Result: &dynamodb.GetItemOutput{
				Item: marshalMap,
			}}, nil

		case *responses.GetMiss:
			// Just log on miss
			d.momentoClient.Logger().Debug("momento lookup did not find key: " + cacheKey)
		}
	}

	d.momentoClient.Logger().Debug("returning DynamoDB response")
	// On MISS Let middleware chains continue, so we can get result and try to cache it
	out, _, err := next.HandleInitialize(ctx, in)

	if err == nil {
		switch o := out.Result.(type) {
		case *dynamodb.GetItemOutput:

			// unmarshal raw response object to DDB attribute values map and encode as json
			j, err := MarshalToJson(o.Item, d.momentoClient.Logger())
			if err != nil {
				return out, err // don't return error
			}

			d.momentoClient.Logger().Debug("caching item with key %s: " + cacheKey)
			// set item in momento cache
			_, err = d.momentoClient.Set(ctx, &momento.SetRequest{
				CacheName: d.cacheName,
				Key:       momento.String(cacheKey),
				Value:     momento.Bytes(j),
			})
			if err != nil {
				d.momentoClient.Logger().Warn(
					fmt.Sprintf("error storing item in cache err=%+v", err),
				)
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

	fieldToValue := make(map[string]string)
	mapKeys := make([]string, 0, len(t))
	for k, v := range t {
		mapKeys = append(mapKeys, k)
		fieldToValue[k] = fmt.Sprintf("|%s|%v|", k, v)
	}
	sort.Strings(mapKeys)

	out := ""
	for _, k := range mapKeys {
		out += fieldToValue[k]
	}

	// prefix key w/ table name and convert to fixed length hash
	hash := sha256.New()
	hash.Write([]byte(tableName + out))
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

func MarshalToJson(item map[string]types.AttributeValue, logger logger.MomentoLogger) ([]byte, error) {
	// unmarshal raw response object to DDB attribute values map
	var t map[string]interface{}
	err := attributevalue.UnmarshalMap(item, &t)
	if err != nil {
		logger.Warn(
			fmt.Sprintf("error decoding output item to store in cache err=%+v", err),
		)
		return nil, fmt.Errorf("error decoding output item to store in cache err=%+v", err)
	}

	// Marshal to JSON to store in cache
	j, err := json.Marshal(t)
	if err != nil {
		logger.Warn(
			fmt.Sprintf("error json encoding new item to store in cache err=%+v", err),
		)
		return nil, fmt.Errorf("error json encoding new item to store in cache err=%+v", err)
	}
	return j, nil
}
