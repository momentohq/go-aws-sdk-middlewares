package caching

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	"github.com/momentohq/go-aws-sdk-middlewares/internal/serializer"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/momentohq/client-sdk-go/momento"
	"github.com/momentohq/client-sdk-go/responses"
)

type WritebackType string

const (
	SYNCHRONOUS  WritebackType = "SYNCHRONOUS"
	ASYNCHRONOUS WritebackType = "ASYNCHRONOUS"
	DISABLED     WritebackType = "DISABLED"
)

type MsgPackSerializer = serializer.MsgPackSerializer
type JSONSerializer = serializer.JSONSerializer

type cachingMiddleware struct {
	cacheName      string
	momentoClient  momento.CacheClient
	writebackType  WritebackType
	asyncWriteChan chan *momento.SetBatchRequest
	serializer     Serializer
}

// Serializer defines the methods for serializing and deserializing data.
type Serializer interface {
	Name() string
	Serialize(item map[string]types.AttributeValue) ([]byte, error)
	Deserialize(data []byte) (map[string]types.AttributeValue, error)
}

type MiddlewareProps struct {
	AwsConfig     *aws.Config
	CacheName     string
	MomentoClient momento.CacheClient
	WritebackType WritebackType
	Serializer    Serializer
}

func AttachNewCachingMiddleware(props MiddlewareProps) {
	if props.WritebackType == "" {
		props.WritebackType = SYNCHRONOUS
	}

	if props.Serializer == nil {
		props.Serializer = serializer.JSONSerializer{}
	}

	props.MomentoClient.Logger().Debug("attaching Momento caching middleware with writeback type " + string(props.WritebackType))
	props.AwsConfig.APIOptions = append(props.AwsConfig.APIOptions, func(stack *middleware.Stack) error {
		return stack.Initialize.Add(
			newCachingMiddleware(&cachingMiddleware{
				cacheName:     props.CacheName,
				momentoClient: props.MomentoClient,
				writebackType: props.WritebackType,
				serializer:    props.Serializer,
			}),
			middleware.Before,
		)
	})
}

func newCachingMiddleware(mw *cachingMiddleware) middleware.InitializeMiddleware {
	if mw.writebackType == ASYNCHRONOUS {
		mw.startAsyncBatchWriter()
	}

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

func (d *cachingMiddleware) startAsyncBatchWriter() {
	const maxBufferSize = 100 // Should we make configurable or larger/smaller?
	batchWriteChan := make(chan *momento.SetBatchRequest, maxBufferSize)

	d.asyncWriteChan = batchWriteChan

	serverContext := context.Background()
	go func() { // TODO we could spawn multiple writers but for now always just have single writer
		// Loop to read from the channel until the server context is done
		for {
			select {
			case batchSetRequest, ok := <-batchWriteChan:
				if !ok {
					d.momentoClient.Logger().Info("async batch write channel closed")
					return
				}
				_, err := d.momentoClient.SetBatch(serverContext, batchSetRequest)
				if err != nil {
					d.momentoClient.Logger().Warn(
						fmt.Sprintf("error storing item batch in cache err=%+v", err),
					)
					// TODO handle limit errors and back off reading items from write chan and possibly retry .
					//var apiErr momento.MomentoError
					//if errors.As(err, &apiErr) {
					//	switch apiErr.Code() {
					//	case momento.LimitExceededError:
					//		// Handle limit error back off.
					//	}
					//}
				}
				d.momentoClient.Logger().Debug(fmt.Sprintf("stored dynamodb items in cache %s", d.cacheName))
			case <-serverContext.Done():
				d.momentoClient.Logger().Info("async batch write channel closed")
				return
			}
		}
	}()

}

func (d *cachingMiddleware) handleBatchGetItemCommand(ctx context.Context, input *dynamodb.BatchGetItemInput, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, error) {
	if len(input.RequestItems) > 100 {
		return middleware.InitializeOutput{}, errors.New("request items exceeded maximum of 100")
	}
	// we gather all responses from both backends in this variable to return to the user as a DDB response
	responsesToReturn := make(map[string][]map[string]types.AttributeValue)
	// this holds the query keys for each DDB table in the request and is used to compute the cache key for
	// cache set operations
	tableToDdbKeys := make(map[string][]string)
	// this holds the computed Momento cache keys for each item in the request, per table
	tableToCacheKeys := make(map[string][]momento.Key)
	cacheMissesPerTable := make(map[string]int)

	// gather cache keys for batch get from Momento cache
	for tableName, keys := range input.RequestItems {
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
			cacheKey, err := ComputeCacheKey(tableName, key, d.serializer)
			if err != nil {
				return middleware.InitializeOutput{}, fmt.Errorf("error getting key for caching: %w", err)
			}
			d.momentoClient.Logger().Debug("computed cache key for batch get retrieval: %s", cacheKey)
			tableToCacheKeys[tableName] = append(tableToCacheKeys[tableName], momento.String(cacheKey))
		}
	}

	gotMiss := false
	gotHit := false

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
					gotHit = true
					deserializedMap, err := d.serializer.Deserialize(e.ValueByte())
					if err != nil {
						return middleware.InitializeOutput{}, fmt.Errorf("error with desrializing map: %w", err)
					}
					responsesToReturn[tableName] = append(responsesToReturn[tableName], deserializedMap)
				case *responses.GetMiss:
					gotMiss = true
					if _, ok := cacheMissesPerTable[tableName]; !ok {
						cacheMissesPerTable[tableName] = 0
					}
					cacheMissesPerTable[tableName]++
					responsesToReturn[tableName] = append(responsesToReturn[tableName], nil)
				}
			}
		}
		if cacheMissesPerTable[tableName] > 0 {
			d.momentoClient.Logger().Debug(fmt.Sprintf("got %d misses for table '%s'", cacheMissesPerTable[tableName], tableName))
		}
	}

	// If we didn't get any misses, we return the entire response from the cache
	if !gotMiss {
		d.momentoClient.Logger().Debug("returning cached batch get responses")
		return middleware.InitializeOutput{
			Result: &dynamodb.BatchGetItemOutput{
				Responses: responsesToReturn,
			},
		}, nil
	}

	// We got some misses, so there's still work for DDB to do
	var newDdbRequest *dynamodb.BatchGetItemInput
	if !gotHit {
		// We didn't get any cache hits, so the new request is the old request
		newDdbRequest = input
	} else {
		// compose a new DDB request with only the cache misses
		newDdbRequest = &dynamodb.BatchGetItemInput{
			RequestItems: map[string]types.KeysAndAttributes{},
		}
		for tableName, keys := range responsesToReturn {
			if _, ok := newDdbRequest.RequestItems[tableName]; !ok {
				newDdbRequest.RequestItems[tableName] = types.KeysAndAttributes{
					Keys: make([]map[string]types.AttributeValue, cacheMissesPerTable[tableName]),
				}
			}
			missIdx := 0
			for idx, key := range keys {
				if key == nil {
					newDdbRequest.RequestItems[tableName].Keys[missIdx] = input.RequestItems[tableName].Keys[idx]
					missIdx++
				}
			}
		}
	}

	// re-issue the DDB request with only the cache misses
	d.momentoClient.Logger().Debug("requesting items from DynamoDB")
	// toReturn will be the final output to return to the user
	toReturn := middleware.InitializeOutput{}
	// replace the original DDB request with the new one
	in.Parameters = newDdbRequest
	out, _, err := next.HandleInitialize(ctx, in)

	if err == nil {
		switch o := out.Result.(type) {
		case *dynamodb.BatchGetItemOutput:
			// check DDB responses and stitch them together with the cache responses
			if !gotHit {
				// if we got all misses, we can just return the DDB response after caching it
				toReturn = out
			} else {
				// Loop through tables we expect responses from
				for tableName, items := range responsesToReturn {
					ddbResponseIdx := 0
					for idx, item := range items {
						// If we had a cache miss then try to grab item from DDB output
						if item == nil {
							// Ensure we actually have item in response in case DDB had
							// error on those keys, and we didn't get an item back
							if o.Responses[tableName] != nil {
								if value, ok := safeGetDDItemFromResponseSlice(o.Responses[tableName], ddbResponseIdx); ok {
									responsesToReturn[tableName][idx] = value
									ddbResponseIdx++
								}
							}
						}
					}
				}
				toReturn = middleware.InitializeOutput{
					Result: &dynamodb.BatchGetItemOutput{
						ConsumedCapacity: o.ConsumedCapacity,
						Responses:        responsesToReturn,
						UnprocessedKeys:  o.UnprocessedKeys,
						ResultMetadata:   middleware.Metadata{},
					},
				}
			}

			if d.writebackType == SYNCHRONOUS {
				d.writeBatchResultsToCache(ctx, o, tableToDdbKeys)
			} else if d.writebackType == ASYNCHRONOUS {
				d.writeBatchResultsToAsyncChannel(o, tableToDdbKeys)
			}

		}
	}

	return toReturn, err
}

func (d *cachingMiddleware) handleGetItemCommand(ctx context.Context, input *dynamodb.GetItemInput, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, error) {
	if input.ConsistentRead != nil {
		return middleware.InitializeOutput{}, errors.New("consistent read not supported with caching middleware")
	}
	if input.TableName == nil {
		return middleware.InitializeOutput{}, errors.New("error table name not set on get-item request")
	}

	// Derive a cache key from DDB request
	cacheKey, err := ComputeCacheKey(*input.TableName, input.Key, d.serializer)
	if err != nil {
		return middleware.InitializeOutput{}, fmt.Errorf("error getting key for caching: %w", err)
	}
	d.momentoClient.Logger().Debug("computed cache key for item retrieval: %s", cacheKey)

	// Try to look up value in momento
	rsp, err := d.momentoClient.Get(ctx, &momento.GetRequest{
		CacheName: d.cacheName,
		Key:       momento.String(cacheKey),
	})
	if err == nil {
		switch r := rsp.(type) {
		case *responses.GetHit:
			// On hit decode value from value stored in cache to a DDB attribute map
			deserializedMap, err := d.serializer.Deserialize(r.ValueByte())
			if err != nil {
				return middleware.InitializeOutput{}, fmt.Errorf("error with marshal map: %w", err)
			}
			d.momentoClient.Logger().Debug("returning cached item")
			// Return user spoofed dynamodb.GetItemOutput.Item w/ cached value
			return struct{ Result interface{} }{Result: &dynamodb.GetItemOutput{
				Item: deserializedMap,
			}}, nil

		case *responses.GetMiss:
			// Just log on miss
			d.momentoClient.Logger().Debug("momento lookup did not find key: " + cacheKey)
		}
	} else {
		d.momentoClient.Logger().Warn(
			fmt.Sprintf("error looking up item in cache err=%+v", err),
		)
	}

	d.momentoClient.Logger().Debug("returning DynamoDB response")
	// On MISS Let middleware chains continue, so we can get result and try to cache it
	out, _, err := next.HandleInitialize(ctx, in)

	if err == nil && d.writebackType != DISABLED {
		switch o := out.Result.(type) {
		case *dynamodb.GetItemOutput:
			if d.writebackType == SYNCHRONOUS {
				d.writeResultToCache(ctx, o, cacheKey)
			} else if d.writebackType == ASYNCHRONOUS {
				go d.writeResultToCache(ctx, o, cacheKey)
			}
		}
	}
	return out, err
}

func (d *cachingMiddleware) writeResultToCache(ctx context.Context, ddbOutput *dynamodb.GetItemOutput, cacheKey string) {
	b, err := d.serializer.Serialize(ddbOutput.Item)
	if err != nil {
		d.momentoClient.Logger().Warn(fmt.Sprintf("error serializing item: %+v", err))
	}

	d.momentoClient.Logger().Debug(fmt.Sprintf("caching item with key: %s", cacheKey))
	// set item in momento cache
	_, err = d.momentoClient.Set(ctx, &momento.SetRequest{
		CacheName: d.cacheName,
		Key:       momento.String(cacheKey),
		Value:     momento.Bytes(b),
	})
	if err != nil {
		d.momentoClient.Logger().Warn(
			fmt.Sprintf("error storing item in cache err=%+v", err),
		)
	}
}

func (d *cachingMiddleware) writeBatchResultsToCache(ctx context.Context, ddbOutput *dynamodb.BatchGetItemOutput, tableToDdbKeys map[string][]string) {
	d.prepareMomentoBatchGetRequest(ddbOutput, tableToDdbKeys)
	// set item batch in Momento cache
	_, err := d.momentoClient.SetBatch(ctx, d.prepareMomentoBatchGetRequest(ddbOutput, tableToDdbKeys))
	if err != nil {
		d.momentoClient.Logger().Warn(
			fmt.Sprintf("error storing item batch in cache err=%+v", err),
		)
	}
	d.momentoClient.Logger().Debug(fmt.Sprintf("stored dynamodb items in cache %s", d.cacheName))
}

func (d *cachingMiddleware) writeBatchResultsToAsyncChannel(ddbOutput *dynamodb.BatchGetItemOutput, tableToDdbKeys map[string][]string) {
	d.asyncWriteChan <- d.prepareMomentoBatchGetRequest(ddbOutput, tableToDdbKeys)
}

func (d *cachingMiddleware) prepareMomentoBatchGetRequest(ddbOutput *dynamodb.BatchGetItemOutput, tableToDdbKeys map[string][]string) *momento.SetBatchRequest {
	d.momentoClient.Logger().Debug("storing dynamodb items in cache")
	var itemsToSet []momento.BatchSetItem
	// compute and gather keys and JSON encoded items to store in Momento cache
	for tableName, items := range ddbOutput.Responses {
		for _, item := range items {
			b, err := d.serializer.Serialize(item)
			if err != nil {
				d.momentoClient.Logger().Warn(fmt.Sprintf("error seralizing item: %+v", err))
				continue
			}

			// extract the keys from the item to compute the hash key
			itemForKey := map[string]types.AttributeValue{}
			for _, key := range tableToDdbKeys[tableName] {
				itemForKey[key] = item[key]
			}
			cacheKey, err := ComputeCacheKey(tableName, itemForKey, d.serializer)
			if err != nil {
				d.momentoClient.Logger().Warn(fmt.Sprintf("error getting key for caching: %+v", err))
				continue
			}
			d.momentoClient.Logger().Debug("computed cache key for batch get storage: %s", cacheKey)

			itemsToSet = append(itemsToSet, momento.BatchSetItem{
				Key:   momento.String(cacheKey),
				Value: momento.Bytes(b),
			})
		}
	}
	return &momento.SetBatchRequest{
		CacheName: d.cacheName,
		Items:     itemsToSet,
	}
}

func ComputeCacheKey(tableName string, keys map[string]types.AttributeValue, serializer Serializer) (string, error) {
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

	// prefix key w/ table name + serializer used and convert to fixed length hash
	hash := sha256.New()
	hash.Write([]byte(tableName + serializer.Name() + out))
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// safeGetDDItemFromResponseSlice safely checks the slice of DDB item responses to avoid a panic
func safeGetDDItemFromResponseSlice(slice []map[string]types.AttributeValue, index int) (map[string]types.AttributeValue, bool) {
	if index >= 0 && index < len(slice) {
		return slice[index], true
	}
	return nil, false
}
