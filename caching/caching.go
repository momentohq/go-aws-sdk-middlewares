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

type WritebackType string

const (
	SYNCHRONOUS  WritebackType = "SYNCHRONOUS"
	ASYNCHRONOUS WritebackType = "ASYNCHRONOUS"
	DISABLED     WritebackType = "DISABLED"
)

type cachingMiddleware struct {
	cacheName      string
	momentoClient  momento.CacheClient
	writebackType  WritebackType
	asyncWriteChan chan *momento.SetBatchRequest
}

type MiddlewareProps struct {
	AwsConfig     *aws.Config
	CacheName     string
	MomentoClient momento.CacheClient
	WritebackType WritebackType
}

func AttachNewCachingMiddleware(props MiddlewareProps) {
	if props.WritebackType == "" {
		props.WritebackType = SYNCHRONOUS
	}
	props.MomentoClient.Logger().Debug("attaching Momento caching middleware with writeback type " + string(props.WritebackType))
	props.AwsConfig.APIOptions = append(props.AwsConfig.APIOptions, func(stack *middleware.Stack) error {
		return stack.Initialize.Add(
			NewCachingMiddleware(&cachingMiddleware{
				cacheName:     props.CacheName,
				momentoClient: props.MomentoClient,
				writebackType: props.WritebackType,
			}),
			middleware.Before,
		)
	})
}

func NewCachingMiddleware(mw *cachingMiddleware) middleware.InitializeMiddleware {
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
	const maxBufferSize = 10000 // TODO think about this number. Should we make configurable or larger/smaller?
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
			cacheKey, err := ComputeCacheKey(tableName, key)
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
					marshalMap, err := GetMarshalMap(e)
					if err != nil {
						return middleware.InitializeOutput{}, fmt.Errorf("error with marshal map: %w", err)
					}
					responsesToReturn[tableName] = append(responsesToReturn[tableName], marshalMap)
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
				for tableName, items := range responsesToReturn {
					ddbResponseIdx := 0
					for idx, item := range items {
						if item == nil {
							responsesToReturn[tableName][idx] = o.Responses[tableName][ddbResponseIdx]
							ddbResponseIdx++
						}
					}
				}
				toReturn = middleware.InitializeOutput{
					Result: &dynamodb.BatchGetItemOutput{
						Responses: responsesToReturn,
					},
				}
			}

			if d.writebackType == SYNCHRONOUS {
				d.writeBatchResultsToCache(ctx, o, tableToDdbKeys)
			} else if d.writebackType == ASYNCHRONOUS {
				go d.writeBatchResultsToCache(ctx, o, tableToDdbKeys)
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
	cacheKey, err := ComputeCacheKey(*input.TableName, input.Key)
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
	// unmarshal raw response object to DDB attribute values map and encode as json
	j, err := MarshalToJson(ddbOutput.Item, d.momentoClient.Logger())
	if err != nil {
		d.momentoClient.Logger().Warn(fmt.Sprintf("error marshalling item to json: %+v", err))
	}

	d.momentoClient.Logger().Debug(fmt.Sprintf("caching item with key: %s", cacheKey))
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
			j, err := MarshalToJson(item, d.momentoClient.Logger())
			if err != nil {
				d.momentoClient.Logger().Warn(fmt.Sprintf("error marshalling item to json: %+v", err))
				continue
			}

			// extract the keys from the item to compute the hash key
			itemForKey := map[string]types.AttributeValue{}
			for _, key := range tableToDdbKeys[tableName] {
				itemForKey[key] = item[key]
			}
			cacheKey, err := ComputeCacheKey(tableName, itemForKey)
			if err != nil {
				d.momentoClient.Logger().Warn(fmt.Sprintf("error getting key for caching: %+v", err))
				continue
			}
			d.momentoClient.Logger().Debug("computed cache key for batch get storage: %s", cacheKey)

			itemsToSet = append(itemsToSet, momento.BatchSetItem{
				Key:   momento.String(cacheKey),
				Value: momento.Bytes(j),
			})
		}
	}
	return &momento.SetBatchRequest{
		CacheName: d.cacheName,
		Items:     itemsToSet,
	}
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
