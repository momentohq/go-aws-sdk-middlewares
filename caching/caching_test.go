package caching

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gorilla/mux"
	"github.com/momentohq/client-sdk-go/momento"
	"github.com/momentohq/client-sdk-go/responses"
)

func TestGetItemCacheMiss(t *testing.T) {
	var (
		keyName                = "testMissKeyName"
		keyValue               = "testMissValue"
		tableName              = "testMissTableName"
		expectedKeyHashValue   = "6fe8b0936a3e8cfd5d6e44e0548096312bc0ceee691def6a558b2ee911a294a2"
		mockMomentoGetResponse = &responses.GetMiss{}
		mockDDBResponse        = fmt.Sprintf(`{"Item": {"%s": { "S": "%s" }}}`, keyName, keyValue)
		expectedGetGalls       = []momento.Key{
			momento.String(expectedKeyHashValue),
		}
		expectedSetCalls = []kvPair{{
			momento.String(expectedKeyHashValue),
			momento.Bytes(fmt.Sprintf(`{"%s":"%s"}`, keyName, keyValue)),
		}}
		mockSetResponse = responses.SetSuccess{}
	)

	// Define Local Mocks used for test
	mmc := &mockMomentoClient{
		mockGetResponses: []responses.GetResponse{
			mockMomentoGetResponse,
		},
		mockSetResponses: []responses.SetResponse{
			mockSetResponse,
		},
	}
	ddbLocal := &mockDDBLocal{
		mockGetItemResponses: []string{
			mockDDBResponse,
		},
	}
	ddbLocal.start()
	amazonConfig := mustGetAWSConfig()

	// Attach Momento Caching Middleware
	AttachNewCachingMiddleware(&amazonConfig, tableName, mmc)

	// Create a DDB client w/ config that has caching middleware attached
	ddbClient := dynamodb.NewFromConfig(amazonConfig)

	// Execute GetItem Request as you would normally
	_, err := ddbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			keyName: &types.AttributeValueMemberS{
				Value: keyValue,
			},
		},
	})
	if err != nil {
		t.Errorf("error occured calling get item: %+v", err)
	}

	if !reflect.DeepEqual(mmc.getCalls, expectedGetGalls) {
		t.Errorf("get not called on momento client with expected keys %+v", mmc.getCalls)
	}

	if !reflect.DeepEqual(mmc.setCalls, expectedSetCalls) {
		t.Errorf("set not called on momento client with expected keys %+v", mmc.setCalls)
	}
}

func TestGetItemHit(t *testing.T) {
	var (
		// !Important: Use consistent values so hash is consistent. No UUID's.
		keyName                = "testHitKeyName"
		keyValue               = "testHitValue"
		tableName              = "testHitTableName"
		expectedKeyHashValue   = "71cbb67599c35874ed2df78ab827698f9fdefce387a013790e43d8d33ac2a693"
		mockMomentoGetResponse = responses.NewGetHit([]byte(fmt.Sprintf(`{"%s":"%s"}`, keyName, keyValue)))
		expectedGetGalls       = []momento.Key{
			momento.String(expectedKeyHashValue),
		}
	)

	ddbLocal := &mockDDBLocal{
		mockGetItemResponses: []string{},
	}
	ddbLocal.start()
	mmc := &mockMomentoClient{
		mockGetResponses: []responses.GetResponse{
			mockMomentoGetResponse,
		},
		mockSetResponses: []responses.SetResponse{},
	}
	aConfig := mustGetAWSConfig()

	// Attach Momento Caching Middleware
	AttachNewCachingMiddleware(&aConfig, tableName, mmc)

	// Create a DDB client
	ddbClient := dynamodb.NewFromConfig(aConfig)

	// Execute GetItem Request as you would normally
	_, err := ddbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			keyName: &types.AttributeValueMemberS{
				Value: keyValue,
			},
		},
	})
	if err != nil {
		t.Errorf("error occured calling get item: %+v", err)
	}

	if !reflect.DeepEqual(mmc.getCalls, expectedGetGalls) {
		t.Errorf("get not called on momento client with expected keys %+v", mmc.getCalls)
	}

	if len(mmc.setCalls) > 0 {
		t.Errorf("set should not be called on cache hit %+v", mmc.setCalls)
	}
	if ddbLocal.getItemCallCount > 0 {
		t.Errorf("DDB should not be called on cache hit callCount=%d", ddbLocal.getItemCallCount)
	}
}

func TestGetItemError(t *testing.T) {
	var (
		// !Important: Use consistent values so hash is consistent. No UUID's.
		keyName                = "testErrorKeyName"
		keyValue               = "testErrorValue"
		tableName              = "testErrorTableName"
		expectedKeyHashValue   = "bce4a132c65e9deb295ea904c64ed2059dd7e369025e15940fe358d0fd818c33"
		mockDDBResponse        = fmt.Sprintf(`{"Item": {"%s": { "S": "%s" }}}`, keyName, keyValue)
		mockMomentoGetResponse = responses.GetMiss{}
		getError               = momento.NewMomentoError(
			"error-code",
			"error-message",
			errors.New("original error"),
		)
		expectedGetGalls = []momento.Key{
			momento.String(expectedKeyHashValue),
		}
		expectedSetCalls []kvPair // we bail on any error currently just let DDB call go as normal
	)

	ddbLocal := &mockDDBLocal{
		mockGetItemResponses: []string{mockDDBResponse},
	}
	ddbLocal.start()
	mmc := &mockMomentoClient{
		mockGetResponses: []responses.GetResponse{
			mockMomentoGetResponse,
		},
		getError: getError,
		mockSetResponses: []responses.SetResponse{
			responses.SetSuccess{},
		},
	}
	aConfig := mustGetAWSConfig()

	// Attach Momento Caching Middleware
	AttachNewCachingMiddleware(&aConfig, tableName, mmc)

	// Create a DDB client
	ddbClient := dynamodb.NewFromConfig(aConfig)

	// Execute GetItem Request as you would normally
	_, err := ddbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			keyName: &types.AttributeValueMemberS{
				Value: keyValue,
			},
		},
	})
	if err != nil {
		t.Errorf("error occured calling get item: %+v", err)
	}

	if !reflect.DeepEqual(mmc.getCalls, expectedGetGalls) {
		t.Errorf("get not called on momento client with expected keys %+v", mmc.getCalls)
	}

	if !reflect.DeepEqual(mmc.setCalls, expectedSetCalls) {
		t.Errorf("set not called on momento client with expected keys %+v", mmc.setCalls)
	}
	if ddbLocal.getItemCallCount != 1 {
		t.Errorf("DDB should be called on cache error callCount=%d", ddbLocal.getItemCallCount)
	}
}

// Test Utils ----------------

// Mock momento Service used for testing
type mockMomentoClient struct {
	momento.CacheClient

	mockGetResponses []responses.GetResponse
	mockSetResponses []responses.SetResponse

	getError error

	getCalls []momento.Key
	setCalls []kvPair
}
type kvPair struct {
	key   momento.Key
	value momento.Bytes
}

func (c *mockMomentoClient) Get(ctx context.Context, r *momento.GetRequest) (responses.GetResponse, error) {
	c.getCalls = append(c.getCalls, r.Key)
	if c.getError != nil {
		return nil, c.getError
	}
	return c.mockGetResponses[0], nil
}

func (c *mockMomentoClient) Set(ctx context.Context, r *momento.SetRequest) (responses.SetResponse, error) {
	c.setCalls = append(c.setCalls, kvPair{r.Key, r.Value.(momento.Bytes)})
	return c.mockSetResponses[0], nil
}

// Mock DDB Local server that's used for testing
type mockDDBLocal struct {
	mockGetItemResponses []string
	getItemCallCount     int
}

func (m *mockDDBLocal) start() {
	go func() {
		r := mux.NewRouter()
		m.getItemCallCount = 0
		r.PathPrefix("/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// obtain target cmd
			target := strings.Split(r.Header["X-Amz-Target"][0], ".")[1]
			switch target {
			case "GetItem":
				_, err := w.Write([]byte(m.mockGetItemResponses[m.getItemCallCount]))
				if err != nil {
					panic(err)
				}
				m.getItemCallCount++
			default:
				panic(errors.New("unsupported ddb method passed: " + target))
			}
		})
		err := http.ListenAndServe(":8080", r)
		if err != nil {
			panic(err)
		}
	}()
}

func mustGetAWSConfig() aws.Config {
	// Set up AWS Config
	cfg, err := awsConfig.LoadDefaultConfig(
		context.TODO(),
	)
	if err != nil {
		panic(err)
	}
	cfg.BaseEndpoint = aws.String("http://localhost:8080")
	cfg.Credentials = credentials.NewStaticCredentialsProvider("dummy", "dummy", "")
	return cfg
}
