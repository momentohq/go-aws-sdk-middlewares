package caching

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
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
	movie           = Movie{Title: "The Big New Movie", Year: 2015}
	getRequestIndex = 0
	setRequestIndex = 0
)

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

func (basics TableBasics) getMovie(title string, year int) (Movie, error) {
	movie := Movie{Title: title, Year: year}
	response, err := basics.DynamoDbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		Key: movie.getKey(), TableName: aws.String(basics.TableName),
	})
	if err != nil {
		log.Printf("Couldn't get info about %v. Here's why: %v\n", title, err)
	} else {
		err = attributevalue.UnmarshalMap(response.Item, &movie)
		if err != nil {
			log.Printf("Couldn't unmarshal response. Here's why: %v\n", err)
		}
	}
	return movie, err
}

func populateDdbLocal() TableBasics {
	config := mustGetAWSConfig()
	ddbClient := dynamodb.NewFromConfig(config)
	tableInfo := TableBasics{DynamoDbClient: ddbClient, TableName: tableName}
	_, err := tableInfo.createTestTable()
	if err != nil {
		panic(err)
	}

	// Add a movie to the table
	err = tableInfo.addMovie(movie)
	if err != nil {
		panic(err)
	}

	return tableInfo
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
	return func() {
		// teardown code here if any
	}
}

func TestLocalDdb(t *testing.T) {
	tableInfo := populateDdbLocal()
	tableInfo.deleteTable()
}

func TestGetItemCacheMiss(t *testing.T) {
	defer setupTest()()
	var (
		expectedKeyHashValue   = "ba805c7ef6e7aa579a8fd513ee73445e8d7a33d05fbf07c25a0a2d9d9a933a68"
		mockMomentoGetResponse = &responses.GetMiss{}
		expectedGetGalls       = []momento.Key{
			momento.String(expectedKeyHashValue),
		}
		expectedSetCalls = []kvPair{{
			momento.String(expectedKeyHashValue),
			momento.Bytes("{\"info\":null,\"title\":\"The Big New Movie\",\"year\":2015}"),
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

	amazonConfig := mustGetAWSConfig()
	// Attach Momento Caching Middleware
	AttachNewCachingMiddleware(&amazonConfig, tableName, mmc)
	ddbClient := dynamodb.NewFromConfig(amazonConfig)

	tableInfo := populateDdbLocal()

	// Execute GetItem Request as you would normally
	_, err := ddbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       movie.getKey(),
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

	tableInfo.deleteTable()
}

func TestGetItemHit(t *testing.T) {
	defer setupTest()()
	var (
		expectedKeyHashValue   = "ba805c7ef6e7aa579a8fd513ee73445e8d7a33d05fbf07c25a0a2d9d9a933a68"
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
	aConfig := mustGetAWSConfig()

	// Attach Momento Caching Middleware
	AttachNewCachingMiddleware(&aConfig, tableName, mmc)

	// Create a DDB client
	ddbClient := dynamodb.NewFromConfig(aConfig)

	// Execute GetItem Request as you would normally
	_, err := ddbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       movie.getKey(),
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
		expectedKeyHashValue   = "ba805c7ef6e7aa579a8fd513ee73445e8d7a33d05fbf07c25a0a2d9d9a933a68"
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

	tableInfo := populateDdbLocal()
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
		Key:       movie.getKey(),
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
	tableInfo.deleteTable()
}

func TestBatchGetItemAllHits(t *testing.T) {
	defer setupTest()()
	var (
		expectedGetCalls = []momento.Key{
			momento.String("2527188ede9e7888721b48523fdec8480a4728f954424b05cc50d515df57fa5d"),
			momento.String("d30fb49db2b22c9c1140467424fd0401b4dc96c9925d75271db072e70090e60e"),
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

	config := mustGetAWSConfig()
	// Attach Momento Caching Middleware
	AttachNewCachingMiddleware(&config, tableName, mmc)
	ddbClient := dynamodb.NewFromConfig(config)
	tableInfo := TableBasics{DynamoDbClient: ddbClient, TableName: tableName}
	_, err := tableInfo.createTestTable()
	if err != nil {
		t.Errorf("error creating table: %+v", err)
	}
	for i := 0; i < 20; i++ {
		err = tableInfo.addMovie(Movie{
			Title: "A Movie Part " + fmt.Sprint(i),
			Year:  2021,
			Info: map[string]interface{}{
				"plot": "Nothing happens at all.",
			},
		})
		if err != nil {
			t.Errorf("error adding data: %+v", err)
		}
	}

	myMovie1 := Movie{
		Title: "A Movie Part 1",
		Year:  2021,
	}
	myMovie2 := Movie{
		Title: "A Movie Part 2",
		Year:  2021,
	}
	req := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			tableName: {
				Keys: []map[string]types.AttributeValue{
					myMovie1.getKey(),
					myMovie2.getKey(),
				},
			},
		},
	}
	response, err := ddbClient.BatchGetItem(context.TODO(), req)
	if err != nil {
		t.Errorf("error occurred calling batch get item: %+v\n", err)
	}

	printResponse(response)

	if !reflect.DeepEqual(mmc.getCalls, expectedGetCalls) {
		t.Errorf("get not called on momento client with expected keys %+v", mmc.getCalls)
	}

	if len(mmc.setCalls) > 0 {
		t.Errorf("set should not be called on cache hit %+v", mmc.setCalls)
	}

	tableInfo.deleteTable()
}

func TestBatchGetItemAllMisses(t *testing.T) {
	defer setupTest()()
	var (
		expectedGetCalls = []momento.Key{
			momento.String("2527188ede9e7888721b48523fdec8480a4728f954424b05cc50d515df57fa5d"),
		}
		expectedSetCalls = []kvPair{
			{
				momento.String("2527188ede9e7888721b48523fdec8480a4728f954424b05cc50d515df57fa5d"),
				momento.Bytes("{\"info\":null,\"title\":\"A Movie Part 1\",\"year\":2021}"),
			},
			{
				momento.String("d30fb49db2b22c9c1140467424fd0401b4dc96c9925d75271db072e70090e60e"),
				momento.Bytes("{\"info\":null,\"title\":\"A Movie Part 2\",\"year\":2021}"),
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

	config := mustGetAWSConfig()
	// Attach Momento Caching Middleware
	AttachNewCachingMiddleware(&config, tableName, mmc)
	ddbClient := dynamodb.NewFromConfig(config)

	tableInfo := TableBasics{DynamoDbClient: ddbClient, TableName: tableName}
	_, err := tableInfo.createTestTable()
	if err != nil {
		t.Errorf("error creating table: %+v", err)
	}
	for i := 0; i < 20; i++ {
		err = tableInfo.addMovie(Movie{
			Title: "A Movie Part " + fmt.Sprint(i),
			Year:  2021,
		})
		if err != nil {
			t.Errorf("error adding data: %+v", err)
		}
	}

	myMovie1 := Movie{
		Title: "A Movie Part 1",
		Year:  2021,
	}
	myMovie2 := Movie{
		Title: "A Movie Part 2",
		Year:  2021,
	}
	req := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			tableName: {
				Keys: []map[string]types.AttributeValue{
					myMovie1.getKey(),
					myMovie2.getKey(),
				},
			},
		},
	}
	response, err := ddbClient.BatchGetItem(context.TODO(), req)
	if err != nil {
		t.Errorf("error occurred calling batch get item: %+v\n", err)
	}

	fmt.Printf("DDB Response: %+v\n", response)

	if !reflect.DeepEqual(mmc.getCalls, expectedGetCalls) {
		t.Errorf("get not called on momento client with expected keys %+v", mmc.getCalls)
	}

	if !reflect.DeepEqual(mmc.setCalls, expectedSetCalls) {
		t.Errorf("set not called on momento client with expected keys %+v", mmc.setCalls)
	}

	tableInfo.deleteTable()
}

// use to inspect dynamo batchGetItem response
func printResponse(response *dynamodb.BatchGetItemOutput) {
	fmt.Printf("DDB Response: %+v\n", response)
	for _, item := range response.Responses[tableName] {
		var movie Movie
		err := attributevalue.UnmarshalMap(item, &movie)
		if err != nil {
			panic(fmt.Sprintf("Couldn't unmarshal response. Here's why: %v\n", err))
		}
		fmt.Printf("Movie: %+v\n", movie)
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
	fmt.Printf("======== GET key: %s\n", r.Key)
	c.getCalls = append(c.getCalls, r.Key)
	if c.getError != nil {
		return nil, c.getError
	}
	rsp := c.mockGetResponses[getRequestIndex]
	getRequestIndex++
	return rsp, nil
}

func (c *mockMomentoClient) Set(ctx context.Context, r *momento.SetRequest) (responses.SetResponse, error) {
	fmt.Printf("======== SET key: %s\n", r.Key)
	fmt.Printf("======== SET value: %s\n", r.Value)
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
