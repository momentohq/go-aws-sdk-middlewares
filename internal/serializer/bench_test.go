package serializer

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type testPayload struct {
	name string
	item map[string]types.AttributeValue
}

var testPayloads = []testPayload{
	{name: "SmallItem", item: smallItem},
	{name: "LargeItem", item: largeSizeItem},
	{name: "ComplexItem", item: complexItem},
}

type Serializer interface {
	Name() string
	Serialize(item map[string]types.AttributeValue) ([]byte, error)
	Deserialize(data []byte) (map[string]types.AttributeValue, error)
}

func BenchmarkSerialization(b *testing.B) {
	serializers := []struct {
		name       string
		serializer Serializer
	}{
		{name: "JSON", serializer: JSONSerializer{}},
		{name: "MsgPack", serializer: MsgPackSerializer{}},
	}

	for _, serializer := range serializers {
		for _, payload := range testPayloads {
			b.Run(serializer.name+"Serialize/"+payload.name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, err := serializer.serializer.Serialize(payload.item)
					if err != nil {
						b.Error(err)
					}
				}
			})

			b.Run(serializer.name+"Deserialize/"+payload.name, func(b *testing.B) {
				data, err := serializer.serializer.Serialize(payload.item)
				if err != nil {
					b.Error(err)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := serializer.serializer.Deserialize(data)
					if err != nil {
						b.Error(err)
					}
				}
			})
		}
	}
}

var smallItem = map[string]types.AttributeValue{
	"info":  &types.AttributeValueMemberNULL{Value: true},
	"title": &types.AttributeValueMemberS{Value: "A Movie Part 1"},
	"year":  &types.AttributeValueMemberN{Value: "2022"},
}

var largeSizeItem = map[string]types.AttributeValue{
	"ID":        &types.AttributeValueMemberN{Value: "123456"},
	"Name":      &types.AttributeValueMemberS{Value: "Test Item"},
	"LargeText": &types.AttributeValueMemberS{Value: strings.Repeat("a", 10_000)},
	"Category":  &types.AttributeValueMemberS{Value: "CategoryA"},
	"Active":    &types.AttributeValueMemberBOOL{Value: true},
}

var complexItem = map[string]types.AttributeValue{
	"Name":    &types.AttributeValueMemberS{Value: "Test Item"},
	"ID":      &types.AttributeValueMemberN{Value: "12345"},
	"Active":  &types.AttributeValueMemberBOOL{Value: true},
	"Tags":    &types.AttributeValueMemberSS{Value: []string{"tag1", "tag2", "tag3"}},
	"Ratings": &types.AttributeValueMemberNS{Value: []string{"5", "4", "3"}},
	"Data":    &types.AttributeValueMemberBS{Value: [][]byte{[]byte("binarydata1"), []byte("binarydata2")}},
	"Attributes": &types.AttributeValueMemberM{
		Value: map[string]types.AttributeValue{
			"Height":  &types.AttributeValueMemberN{Value: "180"},
			"Weight":  &types.AttributeValueMemberN{Value: "75"},
			"Skills":  &types.AttributeValueMemberSS{Value: []string{"skill1", "skill2"}},
			"Scores":  &types.AttributeValueMemberNS{Value: []string{"10", "9.5", "8"}},
			"Active":  &types.AttributeValueMemberBOOL{Value: false},
			"Deleted": &types.AttributeValueMemberNULL{Value: true},
		},
	},
	"Items": &types.AttributeValueMemberL{
		Value: []types.AttributeValue{
			&types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"ItemID":   &types.AttributeValueMemberN{Value: "101"},
					"ItemName": &types.AttributeValueMemberS{Value: "Sub Item 1"},
					"Price":    &types.AttributeValueMemberN{Value: "25.50"},
					"InStock":  &types.AttributeValueMemberBOOL{Value: true},
				},
			},
			&types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"ItemID":   &types.AttributeValueMemberN{Value: "102"},
					"ItemName": &types.AttributeValueMemberS{Value: "Sub Item 2"},
					"Price":    &types.AttributeValueMemberN{Value: "15.75"},
					"InStock":  &types.AttributeValueMemberBOOL{Value: false},
				},
			},
			&types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"ItemID":   &types.AttributeValueMemberN{Value: "103"},
					"ItemName": &types.AttributeValueMemberS{Value: "Sub Item 3"},
					"Price":    &types.AttributeValueMemberN{Value: "100.00"},
					"InStock":  &types.AttributeValueMemberBOOL{Value: true},
				},
			},
		},
	},
	"History": &types.AttributeValueMemberL{
		Value: []types.AttributeValue{
			&types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"Action":    &types.AttributeValueMemberS{Value: "Created"},
					"Timestamp": &types.AttributeValueMemberS{Value: "2023-01-01T12:00:00Z"},
				},
			},
			&types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"Action":    &types.AttributeValueMemberS{Value: "Updated"},
					"Timestamp": &types.AttributeValueMemberS{Value: "2023-03-01T09:30:00Z"},
				},
			},
			&types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"Action":    &types.AttributeValueMemberS{Value: "Deleted"},
					"Timestamp": &types.AttributeValueMemberS{Value: "2023-04-15T16:45:00Z"},
				},
			},
		},
	},
	"RelatedItems": &types.AttributeValueMemberL{
		Value: []types.AttributeValue{
			&types.AttributeValueMemberSS{Value: []string{"relItem1", "relItem2"}},
			&types.AttributeValueMemberSS{Value: []string{"relItem3"}},
		},
	},
	"Notes":    &types.AttributeValueMemberNULL{Value: true},
	"Quantity": &types.AttributeValueMemberN{Value: "50"},
	"Discount": &types.AttributeValueMemberN{Value: "5.00"},
}
