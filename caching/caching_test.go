package caching

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
	tableName       = "movies"
	getRequestIndex = 0
	setRequestIndex = 0
	movie1          = Movie{
		Title: "A Movie Part 1",
		Year:  2021,
	}
	movie2 = Movie{
		Title: "A Movie Part 2",
		Year:  2021,
	}
	movie1hash = "1e21f0974977886cb33d2ca173f89cb9c3c1c5e84712ee07d3fab031817751f2"
	movie2hash = "f334e26f2f40da3172e2dd668a18c58b95b2472a8891ea5a0c63d67ed57c6660"
	movie1json = "{\"info\":null,\"title\":\"A Movie Part 1\",\"year\":2021}"
	movie2json = "{\"info\":null,\"title\":\"A Movie Part 2\",\"year\":2021}"
)

func getDdbClientWithMiddleware(momentoClient momento.CacheClient) *dynamodb.Client {
	amazonConfiguration := mustGetAWSConfig()
	AttachNewCachingMiddleware(&amazonConfiguration, tableName, momentoClient)
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

func setupTest() func() {
	getRequestIndex = 0
	setRequestIndex = 0

	config := mustGetAWSConfig()
	ddbClient := dynamodb.NewFromConfig(config)
	tableInfo := TableBasics{DynamoDbClient: ddbClient, TableName: tableName}
	_, err := tableInfo.createTestTable()
	if err != nil {
		panic(err)
	}
	for i := 0; i < 20; i++ {
		err = tableInfo.addMovie(Movie{
			Title: "A Movie Part " + fmt.Sprint(i),
			Year:  2021,
		})
		if err != nil {
			panic(fmt.Errorf("error adding data: %+v", err))
		}
	}

	// teardown function
	return func() {
		tableInfo.deleteTable()
	}
}

func TestGetItemCacheMiss(t *testing.T) {
	defer setupTest()()
	var (
		expectedKeyHashValue   = movie1hash
		mockMomentoGetResponse = &responses.GetMiss{}
		expectedGetGalls       = []momento.Key{
			momento.String(expectedKeyHashValue),
		}
		expectedSetCalls = []kvPair{{
			momento.String(expectedKeyHashValue),
			momento.Bytes(movie1json),
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
	ddbClient := getDdbClientWithMiddleware(mmc)

	// Execute GetItem Request as you would normally
	_, err := ddbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       movie1.getKey(),
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
	defer setupTest()()
	var (
		expectedKeyHashValue   = movie1hash
		mockMomentoGetResponse = responses.NewGetHit([]byte(fmt.Sprintf(`{"%s":"%s"}`, "foo", "bar")))
		expectedGetCalls       = []momento.Key{
			momento.String(expectedKeyHashValue),
		}
	)

	mmc := &mockMomentoClient{
		mockGetResponses: []responses.GetResponse{
			mockMomentoGetResponse,
		},
		mockSetResponses: []responses.SetResponse{},
	}
	ddbClient := getDdbClientWithMiddleware(mmc)

	// Execute GetItem Request as you would normally
	_, err := ddbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       movie1.getKey(),
	})
	if err != nil {
		t.Errorf("error occured calling get item: %+v", err)
	}

	if !reflect.DeepEqual(mmc.getCalls, expectedGetCalls) {
		t.Errorf("get not called on momento client with expected keys %+v", mmc.getCalls)
	}

	if len(mmc.setCalls) > 0 {
		t.Errorf("set should not be called on cache hit %+v", mmc.setCalls)
	}
}

func TestGetItemError(t *testing.T) {
	defer setupTest()()
	var (
		expectedKeyHashValue   = movie1hash
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

	mmc := &mockMomentoClient{
		mockGetResponses: []responses.GetResponse{
			mockMomentoGetResponse,
		},
		getError: getError,
		mockSetResponses: []responses.SetResponse{
			responses.SetSuccess{},
		},
	}
	ddbClient := getDdbClientWithMiddleware(mmc)

	// Execute GetItem Request as you would normally
	_, err := ddbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       movie1.getKey(),
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

func TestBatchGetItemAllHits(t *testing.T) {
	defer setupTest()()
	var (
		expectedGetCalls = []momento.Key{
			momento.String(movie1hash),
			momento.String(movie2hash),
		}
	)
	// Define Local Mocks used for test
	mmc := &mockMomentoClient{
		mockGetResponses: []responses.GetResponse{
			responses.NewGetHit([]byte(
				fmt.Sprintf(
					`{"%s":"%s", "%s":%s}`,
					"Title", "A Movie Part 1", "Year", "2021",
				),
			)),
			responses.NewGetHit([]byte(
				fmt.Sprintf(
					`{"%s":"%s", "%s":%s}`,
					"Title", "A Movie Part 2", "Year", "2021",
				),
			)),
		},
		mockSetResponses: []responses.SetResponse{},
	}
	ddbClient := getDdbClientWithMiddleware(mmc)

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
	_, err := ddbClient.BatchGetItem(context.TODO(), req)
	if err != nil {
		t.Errorf("error occurred calling batch get item: %+v\n", err)
	}

	if !reflect.DeepEqual(mmc.getCalls, expectedGetCalls) {
		t.Errorf("get not called on momento client with expected keys %+v", mmc.getCalls)
	}

	if len(mmc.setCalls) > 0 {
		t.Errorf("set should not be called on cache hit %+v", mmc.setCalls)
	}
}

func TestBatchGetItemAllMisses(t *testing.T) {
	defer setupTest()()
	var (
		expectedGetCalls = []momento.Key{
			momento.String(movie1hash),
		}
		expectedSetCalls = []kvPair{
			{
				momento.String(movie1hash),
				momento.Bytes(movie1json),
			},
			{
				momento.String(movie2hash),
				momento.Bytes(movie2json),
			},
		}
	)
	// Define Local Mocks used for test
	mmc := &mockMomentoClient{
		mockGetResponses: []responses.GetResponse{
			&responses.GetMiss{},
			&responses.GetMiss{},
		},
		mockSetResponses: []responses.SetResponse{
			&responses.SetSuccess{},
			&responses.SetSuccess{},
		},
	}
	ddbClient := getDdbClientWithMiddleware(mmc)

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
	_, err := ddbClient.BatchGetItem(context.TODO(), req)
	if err != nil {
		t.Errorf("error occurred calling batch get item: %+v\n", err)
	}

	if !reflect.DeepEqual(mmc.getCalls, expectedGetCalls) {
		t.Errorf("get not called on momento client with expected keys %+v", mmc.getCalls)
	}

	if !reflect.DeepEqual(mmc.setCalls, expectedSetCalls) {
		t.Errorf("set not called on momento client with expected keys %+v", mmc.setCalls)
	}
}

func TestBatchGetItemsMixed(t *testing.T) {
	defer setupTest()()
	var (
		expectedGetCalls = []momento.Key{
			momento.String(movie1hash),
			momento.String(movie2hash),
		}
		expectedSetCalls = []kvPair{
			{
				momento.String(movie1hash),
				momento.Bytes(movie1json),
			},
			{
				momento.String(movie2hash),
				momento.Bytes(movie2json),
			},
		}
	)

	mmc := &mockMomentoClient{
		mockGetResponses: []responses.GetResponse{
			responses.NewGetHit([]byte(
				fmt.Sprintf(
					`{"%s":"%s", "%s":%s}`,
					"Title", "A Movie Part 1", "Year", "2021",
				),
			)),
			&responses.GetMiss{},
		},
		mockSetResponses: []responses.SetResponse{
			&responses.SetSuccess{},
			&responses.SetSuccess{},
		},
	}
	ddbClient := getDdbClientWithMiddleware(mmc)

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
	_, err := ddbClient.BatchGetItem(context.TODO(), req)
	if err != nil {
		t.Errorf("error occurred calling batch get item: %+v\n", err)
	}

	if !reflect.DeepEqual(mmc.getCalls, expectedGetCalls) {
		t.Errorf("get not called on momento client with expected keys %+v", mmc.getCalls)
	}

	if !reflect.DeepEqual(mmc.setCalls, expectedSetCalls) {
		t.Errorf("set not called on momento client with expected keys %+v", mmc.setCalls)
	}
}

func TestBatchGetItemsError(t *testing.T) {
	defer setupTest()()
	var (
		expectedKeyHashValue   = movie1hash
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

	mmc := &mockMomentoClient{
		mockGetResponses: []responses.GetResponse{
			mockMomentoGetResponse,
		},
		getError: getError,
		mockSetResponses: []responses.SetResponse{
			responses.SetSuccess{},
			responses.SetSuccess{},
		},
	}
	ddbClient := getDdbClientWithMiddleware(mmc)

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

	_, err := ddbClient.BatchGetItem(context.TODO(), req)
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
	rsp := c.mockGetResponses[getRequestIndex]
	getRequestIndex++
	return rsp, nil
}

func (c *mockMomentoClient) Set(ctx context.Context, r *momento.SetRequest) (responses.SetResponse, error) {
	c.setCalls = append(c.setCalls, kvPair{r.Key, r.Value.(momento.Bytes)})
	rsp := c.mockSetResponses[setRequestIndex]
	setRequestIndex++
	return rsp, nil
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
