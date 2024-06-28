package caching

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/momentohq/client-sdk-go/auth"
	"github.com/momentohq/client-sdk-go/config"
	"github.com/momentohq/client-sdk-go/config/logger"
	"github.com/momentohq/client-sdk-go/config/logger/momento_default_logger"
	"github.com/momentohq/client-sdk-go/momento"
	"github.com/momentohq/client-sdk-go/responses"
)

type TableBasics struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
}

type Movie struct {
	Title string                 `dynamodbav:"title"`
	Year  int                    `dynamodbav:"year"`
	Info  map[string]interface{} `dynamodbav:"info"`
}

var (
	momentoClient  momento.CacheClient
	ddbClient      *dynamodb.Client
	tableInfo      TableBasics
	tableName      string
	movies         []Movie
	movie1         Movie
	movie2         Movie
	movie1hash     string
	movie2hash     string
	movie1json2022 = "{\"info\":null,\"title\":\"A Movie Part 1\",\"year\":2022}"
	movie2json2022 = "{\"info\":null,\"title\":\"A Movie Part 2\",\"year\":2022}"
	writebackType  = SYNCHRONOUS
)

func setupTest() func() {
	tableName = fmt.Sprintf("movies-%s", uuid.NewString())
	credProvider, err := auth.NewEnvMomentoTokenProvider("MOMENTO_API_KEY")
	if err != nil {
		panic(err)
	}
	momentoClient, err = momento.NewCacheClient(
		config.LaptopLatestWithLogger(
			momento_default_logger.NewDefaultMomentoLoggerFactory(momento_default_logger.DEBUG),
		),
		credProvider,
		60*time.Second,
	)
	if err != nil {
		panic(err)
	}
	_, err = momentoClient.CreateCache(context.Background(), &momento.CreateCacheRequest{
		CacheName: tableName,
	})
	if err != nil {
		panic(err)
	}

	// writebackType defaults to synchronous but can be modified before calling `setupTest()`
	// you may also instantiate additional clients to test, passing different values for writebackType
	// to `getDdbClientWithMiddleware()`
	ddbClient = getDdbClientWithMiddleware(momentoClient, &writebackType)

	amazonConfig := mustGetAWSConfig()
	ddbControlClient := dynamodb.NewFromConfig(amazonConfig)
	tableInfo = TableBasics{DynamoDbClient: ddbControlClient, TableName: tableName}

	momentoClient.Logger().Debug("Populating DDB with movies")
	_, err = tableInfo.createTestTable()
	if err != nil {
		panic(err)
	}
	// insert movies in DDB with year = 2021
	for i := 0; i < 50; i++ {
		movie := Movie{
			Title: "A Movie Part " + fmt.Sprint(i+1),
			Year:  2021,
		}
		err = tableInfo.addMovie(movie)
		if err != nil {
			panic(fmt.Errorf("error adding data: %+v", err))
		}
		movies = append(movies, movie)
	}
	momentoClient.Logger().Debug("done populating data")

	movie1 = movies[0]
	movie2 = movies[1]
	movie1hash, err = ComputeCacheKey(tableName, movie1.getKey())
	if err != nil {
		panic(err)
	}
	movie2hash, err = ComputeCacheKey(tableName, movie2.getKey())
	if err != nil {
		panic(err)
	}

	// teardown function
	return func() {
		tableInfo.deleteTable()
		_, err := momentoClient.DeleteCache(context.Background(), &momento.DeleteCacheRequest{
			CacheName: tableName,
		})
		if err != nil {
			panic(err)
		}
		momentoClient.Close()
		writebackType = SYNCHRONOUS
	}
}

// cache miss tests
func testGetItemCacheMissCommon(t *testing.T) (Movie, responses.GetResponse) {
	// Execute GetItem Request as you would normally
	resp, err := ddbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       movie1.getKey(),
	})
	if err != nil {
		t.Errorf("error occured calling ddb get item: %+v", err)
	}

	movie, err := getMovieFromDdbItem(resp.Item)
	if err != nil {
		t.Errorf("error decoding dynamodb response: %+v", err)
	}

	time.Sleep(1 * time.Second)
	getResp, err := momentoClient.Get(context.Background(), &momento.GetRequest{
		CacheName: tableName,
		Key:       momento.String(movie1hash),
	})
	if err != nil {
		t.Errorf("error occured calling momento get: %+v", err)
	}
	return movie, getResp
}

func TestGetItemCacheMiss(t *testing.T) {
	defer setupTest()()

	movie, getResp := testGetItemCacheMissCommon(t)
	switch r := getResp.(type) {
	case *responses.GetHit:
		movieInfo, err := getMapFromJsonBytes(r.ValueByte())
		if err != nil {
			t.Errorf("error decoding cache hit: %+v", err)
		}
		if movieInfo["title"] != movie.Title {
			t.Errorf("expected cache hit title to match dynamodb response: %+v != %+v", movieInfo, movie)
		}
		if fmt.Sprint(movieInfo["year"]) != fmt.Sprint(movie.Year) {
			t.Errorf("expected cache hit year to match dynamodb response: %+v != %+v", movieInfo, movie)
		}
	case *responses.GetMiss:
		t.Errorf("expected cache hit, got cache miss for key %s", movie1hash)
	}
}

func TestGetItemCacheMissAsync(t *testing.T) {
	writebackType = ASYNCHRONOUS
	TestGetItemCacheMiss(t)
}

func TestGetItemCacheMissNoWriteback(t *testing.T) {
	writebackType = DISABLED
	defer setupTest()()
	_, getResp := testGetItemCacheMissCommon(t)
	switch getResp.(type) {
	case *responses.GetHit:
		t.Errorf("expected cache miss, got cache hit")
	}
}

// cache hit tests
func TestGetItemCacheHitAsync(t *testing.T) {
	writebackType = ASYNCHRONOUS
	TestGetItemCacheHit(t)
}

func TestGetItemCacheHit(t *testing.T) {
	defer setupTest()()

	_, err := momentoClient.Set(context.Background(), &momento.SetRequest{
		CacheName: tableName,
		Key:       momento.String(movie1hash),
		Value:     momento.Bytes(movie1json2022),
	})
	if err != nil {
		t.Errorf("error occured calling momento set: %+v", err)
	}

	// Execute GetItem Request as you would normally
	resp, err := ddbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       movie1.getKey(),
	})
	if err != nil {
		t.Errorf("error occured calling get item: %+v", err)
	}

	movie, err := getMovieFromDdbItem(resp.Item)
	if err != nil {
		t.Errorf("error decoding dynamodb response: %+v", err)
	}

	if movie.Year != 2022 {
		t.Errorf("expected cache hit year to be 2022: %+v", movie)
	}
}

// cache error test
func TestGetItemError(t *testing.T) {
	defer setupTest()()
	mmc := &mockMomentoClient{}
	ddbClient := getDdbClientWithMiddleware(mmc, nil)

	// Execute GetItem Request as you would normally
	resp, err := ddbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       movie1.getKey(),
	})
	if err != nil {
		t.Errorf("error occured calling get item: %+v", err)
	}

	movie, err := getMovieFromDdbItem(resp.Item)
	if err != nil {
		t.Errorf("error decoding dynamodb response: %+v", err)
	}
	momentoClient.Logger().Debug(fmt.Sprintf("movie: %+v", movie))
	if movie.Year != 2021 {
		t.Errorf("expected ddb hit year to be 2021: %+v", movie)
	}
}

// batch get tests - hits
func TestBatchGetItemAllHits(t *testing.T) {
	defer setupTest()()

	_, err := momentoClient.SetBatch(context.Background(), &momento.SetBatchRequest{
		CacheName: tableName,
		Items: []momento.BatchSetItem{
			{
				Key:   momento.String(movie1hash),
				Value: momento.Bytes(movie1json2022),
			},
			{
				Key:   momento.String(movie2hash),
				Value: momento.Bytes(movie2json2022),
			},
		},
	})
	if err != nil {
		t.Errorf("error occured calling momento set: %+v", err)
	}

	req := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			tableName: {
				Keys: []map[string]types.AttributeValue{
					movie1.getKey(),
					movie2.getKey(),
				},
			},
		},
	}
	resp, err := ddbClient.BatchGetItem(context.TODO(), req)
	if err != nil {
		t.Errorf("error occurred calling batch get item: %+v\n", err)
	}
	for _, items := range resp.Responses {
		for _, item := range items {
			movie, err := getMovieFromDdbItem(item)
			if err != nil {
				t.Errorf("error decoding dynamodb response: %+v", err)
			}
			if movie.Year != 2022 {
				t.Errorf("expected cache hit year to be 2022: %+v", movie)
			}
		}
	}
}

func TestBatchGetItemAllHitsAsync(t *testing.T) {
	writebackType = ASYNCHRONOUS
	TestBatchGetItemAllHits(t)
}

// batch get tests - misses
func testBatchGetItemAllMissesCommon(t *testing.T) responses.GetBatchResponse {
	defer setupTest()()

	req := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			tableName: {
				Keys: []map[string]types.AttributeValue{
					movie1.getKey(),
					movie2.getKey(),
				},
			},
		},
	}
	resp, err := ddbClient.BatchGetItem(context.TODO(), req)
	if err != nil {
		t.Errorf("error occurred calling batch get item: %+v\n", err)
	}
	for _, items := range resp.Responses {
		for _, item := range items {
			movie, err := getMovieFromDdbItem(item)
			if err != nil {
				t.Errorf("error decoding dynamodb response: %+v", err)
			}
			if movie.Year != 2021 {
				t.Errorf("expected ddb hit year to be 2021: %+v", movie)
			}
		}
	}

	// give the middleware goroutine a little time to finish caching DDB data for the Momento misses
	time.Sleep(1 * time.Second)

	// make sure results were set in Momento cache
	getResp, err := momentoClient.GetBatch(context.Background(), &momento.GetBatchRequest{
		CacheName: tableName,
		Keys: []momento.Key{
			momento.String(movie1hash),
			momento.String(movie2hash),
		},
	})
	if err != nil {
		t.Errorf("error occured calling momento get: %+v", err)
	}
	return getResp
}

func TestBatchGetItemAllMisses(t *testing.T) {
	getResp := testBatchGetItemAllMissesCommon(t)
	switch r := getResp.(type) {
	case responses.GetBatchSuccess:
		for _, element := range r.Results() {
			switch e := element.(type) {
			case *responses.GetHit:
				movieInfo, err := getMapFromJsonBytes(e.ValueByte())
				if err != nil {
					t.Errorf("error decoding cache hit: %+v", err)
				}
				if fmt.Sprint(movieInfo["year"]) != fmt.Sprint(2021) {
					t.Errorf("expected cache hit year to match ddb response: %+v", movieInfo)
				}
			case *responses.GetMiss:
				t.Errorf("expected cache hit, got cache miss")
			}
		}
	default:
		t.Errorf("unknown get batch response type: %T\n", r)
	}
}

func TestBatchGetItemAllMissesAsync(t *testing.T) {
	writebackType = ASYNCHRONOUS
	TestBatchGetItemAllMisses(t)
}

func TestBatchGetItemAllMissesNoWriteback(t *testing.T) {
	writebackType = DISABLED
	getResp := testBatchGetItemAllMissesCommon(t)
	switch r := getResp.(type) {
	case responses.GetBatchSuccess:
		for _, element := range r.Results() {
			switch element.(type) {
			case *responses.GetHit:
				t.Errorf("expected cache hit, got cache miss")
			}
		}
	default:
		t.Errorf("unknown get batch response type: %T\n", r)
	}
}

// batch get tests - mixed hits and misses
func testBatchGetItemsMixedCommon(t *testing.T) responses.GetBatchResponse {
	_, err := momentoClient.Set(context.Background(), &momento.SetRequest{
		CacheName: tableName,
		Key:       momento.String(movie1hash),
		Value:     momento.Bytes(movie1json2022),
	})
	if err != nil {
		t.Errorf("error occured calling momento set: %+v", err)
	}

	req := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			tableName: {
				Keys: []map[string]types.AttributeValue{
					movie1.getKey(),
					movie2.getKey(),
				},
			},
		},
	}
	time.Sleep(1 * time.Second)
	resp, err := ddbClient.BatchGetItem(context.TODO(), req)
	if err != nil {
		t.Errorf("error occurred calling batch get item: %+v\n", err)
	}

	for _, items := range resp.Responses {
		for _, item := range items {
			movie, err := getMovieFromDdbItem(item)
			if err != nil {
				t.Errorf("error decoding dynamodb response: %+v", err)
			}
			if movie.Title == "A Movie Part 1" && movie.Year != 2022 {
				t.Errorf("expected cache hit year to be 2022: %+v", movie)
			}
			if movie.Title == "A Movie Part 2" && movie.Year != 2021 {
				t.Errorf("expected ddb hit year to be 2021: %+v", movie)
			}
		}
	}

	// give the middleware goroutine a little time to finish caching DDB data for the Momento misses
	time.Sleep(500 * time.Millisecond)

	// make sure cached versions were overwritten/written
	getResp, err := momentoClient.GetBatch(context.Background(), &momento.GetBatchRequest{
		CacheName: tableName,
		Keys: []momento.Key{
			momento.String(movie1hash),
			momento.String(movie2hash),
		},
	})
	if err != nil {
		t.Errorf("error occured calling momento get: %+v", err)
	}
	return getResp
}

func TestBatchGetItemsMixed(t *testing.T) {
	defer setupTest()()
	getResp := testBatchGetItemsMixedCommon(t)
	switch r := getResp.(type) {
	case responses.GetBatchSuccess:
		for _, element := range r.Results() {
			switch e := element.(type) {
			case *responses.GetHit:
				movieInfo, err := getMapFromJsonBytes(e.ValueByte())
				if err != nil {
					t.Errorf("error decoding cache hit: %+v", err)
				}
				if movieInfo["title"] == "A Movie Part 1" && fmt.Sprint(movieInfo["year"]) != fmt.Sprint(2022) {
					t.Errorf("expected cache hit year to match ddb response: %+v", movieInfo)
				}
				if movieInfo["title"] == "A Movie Part 2" && fmt.Sprint(movieInfo["year"]) != fmt.Sprint(2021) {
					t.Errorf("expected ddb hit year: %+v", movieInfo)
				}
			case *responses.GetMiss:
				t.Errorf("expected cache hit, got cache miss")
			}
		}
	}
}

func TestBatchGetItemsMixedAsync(t *testing.T) {
	writebackType = ASYNCHRONOUS
	TestBatchGetItemsMixed(t)
}

func TestBatchGetItemsMixedNoWriteback(t *testing.T) {
	writebackType = DISABLED
	defer setupTest()()
	getResp := testBatchGetItemsMixedCommon(t)
	switch r := getResp.(type) {
	case responses.GetBatchSuccess:
		for _, element := range r.Results() {
			switch e := element.(type) {
			case *responses.GetHit:
				movieInfo, err := getMapFromJsonBytes(e.ValueByte())
				if err != nil {
					t.Errorf("error decoding cache hit: %+v", err)
				}
				if movieInfo["title"] != "A Movie Part 1" {
					t.Errorf("expected cache miss but got: %+v", movieInfo)
				}
			}
		}
	}
}

// batch get test with error
func TestBatchGetItemsError(t *testing.T) {
	defer setupTest()()
	mmc := &mockMomentoClient{}
	ddbClient := getDdbClientWithMiddleware(mmc, nil)

	req := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			tableName: {
				Keys: []map[string]types.AttributeValue{
					movie1.getKey(),
					movie2.getKey(),
				},
			},
		},
	}

	resp, err := ddbClient.BatchGetItem(context.TODO(), req)
	if err != nil {
		t.Errorf("error occured calling get item: %+v", err)
	}
	// Momento client errored out, so we should get DDB data
	for _, items := range resp.Responses {
		for _, item := range items {
			movie, err := getMovieFromDdbItem(item)
			if err != nil {
				t.Errorf("error decoding dynamodb response: %+v", err)
			}
			if movie.Year != 2021 {
				t.Errorf("expected ddb hit year to be 2021: %+v", movie)
			}
		}
	}
}

// Test Utils ----------------

// Mock momento Service used for testing
type mockMomentoClient struct {
	momento.CacheClient
}

func (c *mockMomentoClient) Logger() logger.MomentoLogger {
	return momento_default_logger.NewDefaultMomentoLoggerFactory(momento_default_logger.DEBUG).GetLogger("mock-momento-client")
}

func (c *mockMomentoClient) Get(_ context.Context, r *momento.GetRequest) (responses.GetResponse, error) {
	return nil, momento.NewMomentoError("error-code", "error-message", errors.New("original error"))
}

func (c *mockMomentoClient) GetBatch(_ context.Context, r *momento.GetBatchRequest) (responses.GetBatchResponse, error) {
	return nil, momento.NewMomentoError("error-code", "error-message", errors.New("original error"))
}

func (c *mockMomentoClient) Set(_ context.Context, r *momento.SetRequest) (responses.SetResponse, error) {
	return nil, momento.NewMomentoError("error-code", "error-message", errors.New("original error"))
}

func mustGetAWSConfig() aws.Config {
	// Set up AWS Config
	cfg, err := awsConfig.LoadDefaultConfig(
		context.TODO(),
	)
	if err != nil {
		panic(err)
	}
	// DynamoDB Local Docker container endpoint
	cfg.BaseEndpoint = aws.String("http://localhost:8000")
	cfg.Credentials = credentials.NewStaticCredentialsProvider("dummy", "dummy", "")
	return cfg
}

func getMapFromJsonBytes(jsonBytes []byte) (map[string]interface{}, error) {
	var myMap map[string]interface{}
	err := json.NewDecoder(bytes.NewReader(jsonBytes)).Decode(&myMap)
	if err != nil {
		return nil, err
	}
	return myMap, nil
}

func getMovieFromDdbItem(item map[string]types.AttributeValue) (Movie, error) {
	var movie Movie
	err := attributevalue.UnmarshalMap(item, &movie)
	if err != nil {
		return movie, err
	}
	return movie, nil
}

func getDdbClientWithMiddleware(momentoClient momento.CacheClient, writebackType *WritebackType) *dynamodb.Client {
	amazonConfiguration := mustGetAWSConfig()
	var wb WritebackType
	if writebackType != nil {
		wb = *writebackType
	}
	AttachNewCachingMiddleware(MiddlewareProps{
		&amazonConfiguration,
		tableName,
		momentoClient,
		wb,
	})
	return dynamodb.NewFromConfig(amazonConfiguration)
}

func (basics TableBasics) createTestTable() (*types.TableDescription, error) {
	var tableDesc *types.TableDescription
	table, err := basics.DynamoDbClient.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{{
			AttributeName: aws.String("year"),
			AttributeType: types.ScalarAttributeTypeN,
		}, {
			AttributeName: aws.String("title"),
			AttributeType: types.ScalarAttributeTypeS,
		}},
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String("year"),
			KeyType:       types.KeyTypeHash,
		}, {
			AttributeName: aws.String("title"),
			KeyType:       types.KeyTypeRange,
		}},
		TableName: aws.String(basics.TableName),
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
	})
	if err != nil {
		log.Printf("Couldn't create table %v. Here's why: %v\n", basics.TableName, err)
	} else {
		waiter := dynamodb.NewTableExistsWaiter(basics.DynamoDbClient)
		err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(basics.TableName)}, 5*time.Minute)
		if err != nil {
			log.Printf("Wait for table exists failed. Here's why: %v\n", err)
		}
		tableDesc = table.TableDescription
	}
	return tableDesc, err
}

func (basics TableBasics) addMovie(movie Movie) error {
	item, err := attributevalue.MarshalMap(movie)
	if err != nil {
		panic(err)
	}
	_, err = basics.DynamoDbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(basics.TableName), Item: item,
	})
	if err != nil {
		log.Printf("Couldn't add item to table. Here's why: %v\n", err)
	}
	return err
}

func (movie Movie) getKey() map[string]types.AttributeValue {
	title, err := attributevalue.Marshal(movie.Title)
	if err != nil {
		panic(err)
	}
	year, err := attributevalue.Marshal(movie.Year)
	if err != nil {
		panic(err)
	}
	return map[string]types.AttributeValue{"title": title, "year": year}
}

func (basics TableBasics) deleteTable() {
	_, err := basics.DynamoDbClient.DeleteTable(context.TODO(), &dynamodb.DeleteTableInput{
		TableName: aws.String(basics.TableName),
	})
	if err != nil {
		log.Printf("Couldn't delete table %v. Here's why: %v\n", basics.TableName, err)
	}
}
