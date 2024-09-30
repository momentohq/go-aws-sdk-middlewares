package serializer

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/vmihailenco/msgpack/v5"
)

type MsgPackSerializer struct{}

func (d MsgPackSerializer) Name() string {
	return "MsgPack"
}

func (d MsgPackSerializer) Serialize(item map[string]types.AttributeValue) ([]byte, error) {
	var t map[string]interface{}
	err := attributevalue.UnmarshalMap(item, &t)
	if err != nil {
		panic(err)
	}
	return msgpack.Marshal(t)
}

func (d MsgPackSerializer) Deserialize(data []byte) (map[string]types.AttributeValue, error) {
	var item map[string]interface{}
	err := msgpack.Unmarshal(data, &item)
	if err != nil {
		panic(err)
	}
	return attributevalue.MarshalMap(item)
}

type JSONSerializer struct{}

func (d JSONSerializer) Name() string {
	return "JSON"
}

func (d JSONSerializer) Serialize(item map[string]types.AttributeValue) ([]byte, error) {
	// unmarshal raw response object to DDB attribute values map
	var t map[string]interface{}
	err := attributevalue.UnmarshalMap(item, &t)
	if err != nil {
		return nil, fmt.Errorf("error decoding output item to store in cache err=%+v", err)
	}

	// Marshal to JSON to store in cache
	j, err := json.Marshal(t)
	if err != nil {
		return nil, fmt.Errorf("error json encoding new item to store in cache err=%+v", err)
	}
	return j, nil
}

func (d JSONSerializer) Deserialize(data []byte) (map[string]types.AttributeValue, error) {
	var t map[string]interface{}
	err := json.NewDecoder(bytes.NewReader(data)).Decode(&t)
	if err != nil {
		return nil, fmt.Errorf("error decoding json item in cache to return: %w", err)
	}

	marshalMap, err := attributevalue.MarshalMap(t)
	if err != nil {
		return nil, fmt.Errorf("error encoding item in cache to ddbItem to return: %w", err)
	}
	return marshalMap, nil
}
