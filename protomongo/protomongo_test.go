package protomongo

import (
	"reflect"
	"testing"

	"github.com/dataform-co/dataform/protos/dataform"
	"github.com/golang/protobuf/proto"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	tests = []struct {
		name string
		pb   proto.Message
		new  func() proto.Message
	}{
		{
			name: "simple message",
			pb: &dataform.ProjectConfig{
				Warehouse:               "foo",
				DefaultSchema:           "bar",
				IdempotentActionRetries: 32,
			},
			new: func() proto.Message {
				return new(dataform.ProjectConfig)
			},
		},
		{
			name: "message with repeated fields",
			pb: &dataform.RunConfig{
				Actions:     []string{"foo", "bar"},
				Tags:        []string{"baz", "qux"},
				FullRefresh: true,
			},
			new: func() proto.Message {
				return new(dataform.RunConfig)
			},
		},
		{
			name: "message with submessage",
			pb: &dataform.CompileConfig{
				ProjectDir: "foo",
				ProjectConfigOverride: &dataform.ProjectConfig{
					Warehouse:               "bar",
					DefaultSchema:           "baz",
					IdempotentActionRetries: 16,
				},
			},
			new: func() proto.Message {
				return new(dataform.CompileConfig)
			},
		},
		{
			name: "message with repeated submessage",
			pb: &dataform.ActionDescriptor{
				Description: "foo",
				Columns: []*dataform.ColumnDescriptor{
					&dataform.ColumnDescriptor{
						Description: "bar",
					},
					&dataform.ColumnDescriptor{
						Description: "baz",
					},
				},
			},
			new: func() proto.Message {
				return new(dataform.ActionDescriptor)
			},
		},
		{
			name: "message with oneof",
			pb: &dataform.Field{
				Name: "foo",
				Type: &dataform.Field_Primitive{
					"bar",
				},
			},
			new: func() proto.Message {
				return new(dataform.Field)
			},
		},
	}
)

func TestMarshalUnmarshal(t *testing.T) {
	rb := bson.NewRegistryBuilder()
	rb.RegisterCodec(reflect.TypeOf((*proto.Message)(nil)).Elem(), &ProtobufCodec{})
	reg := rb.Build()

	for _, testCase := range tests {
		b, err := bson.MarshalWithRegistry(reg, testCase.pb)
		if err != nil {
			t.Errorf("bson.MarshalWithRegistry error = %v", err)
		}
		out := testCase.new()
		if err = bson.UnmarshalWithRegistry(reg, b, &out); err != nil {
			t.Errorf("bson.UnmarshalWithRegistry error = %v", err)
		}
		if !proto.Equal(testCase.pb, out) {
			t.Errorf("failed: in=%#v, out=%#v", testCase.pb, out)
		}
	}
}

func TestMarshalUnmarshalWithPointers(t *testing.T) {
	rb := bson.NewRegistryBuilder()
	rb.RegisterCodec(reflect.TypeOf((*proto.Message)(nil)).Elem(), &ProtobufCodec{})
	reg := rb.Build()

	for _, testCase := range tests {
		b, err := bson.MarshalWithRegistry(reg, &testCase.pb)
		if err != nil {
			t.Errorf("bson.MarshalWithRegistry error = %v", err)
		}
		out := testCase.new()
		if err = bson.UnmarshalWithRegistry(reg, b, &out); err != nil {
			t.Errorf("bson.UnmarshalWithRegistry error = %v", err)
		}
		if !proto.Equal(testCase.pb, out) {
			t.Errorf("failed: in=%#v, out=%#v", testCase.pb, out)
		}
	}
}
