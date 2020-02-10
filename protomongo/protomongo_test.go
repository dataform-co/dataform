package protomongo

import (
	"reflect"
	"testing"

	pb "github.com/dataform-co/dataform/protomongo/example"
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
			pb: &pb.SimpleMessage{
				StringField: "foo",
				Int32Field:  32525,
				Int64Field:  1531541553141312315,
				FloatField:  21541.3242,
				DoubleField: 21535215136361617136.543858,
				BoolField:   true,
				EnumField:   pb.Enum_VAL_2,
			},
			new: func() proto.Message {
				return new(pb.SimpleMessage)
			},
		},
		{
			name: "message with repeated fields",
			pb: &pb.RepeatedFieldMessage{
				StringField: []string{"foo", "bar"},
				Int32Field:  []int32{32525, 1958, 435},
				Int64Field:  []int64{1531541553141312315, 13512516266},
				FloatField:  []float32{21541.3242, 634214.2233, 3435.322},
				DoubleField: []float64{21535215136361617136.543858, 213143343.76767},
				BoolField:   []bool{true, false, true, true},
				EnumField:   []pb.Enum{pb.Enum_VAL_2, pb.Enum_VAL_1},
			},
			new: func() proto.Message {
				return new(pb.RepeatedFieldMessage)
			},
		},
		{
			name: "message with submessage",
			pb: &pb.MessageWithSubMessage{
				StringField: "baz",
				SimpleMessage: &pb.SimpleMessage{
					StringField: "foo",
					Int32Field:  32525,
					Int64Field:  1531541553141312315,
					FloatField:  21541.3242,
					DoubleField: 21535215136361617136.543858,
					BoolField:   true,
					EnumField:   pb.Enum_VAL_2,
				},
			},
			new: func() proto.Message {
				return new(pb.MessageWithSubMessage)
			},
		},
		{
			name: "message with repeated submessage",
			pb: &pb.MessageWithRepeatedSubMessage{
				StringField: "baz",
				SimpleMessage: []*pb.SimpleMessage{
					&pb.SimpleMessage{
						StringField: "foo",
						Int32Field:  32525,
						Int64Field:  1531541553141312315,
						FloatField:  21541.3242,
						DoubleField: 21535215136361617136.543858,
						BoolField:   true,
						EnumField:   pb.Enum_VAL_2,
					},
					&pb.SimpleMessage{
						StringField: "qux",
						Int32Field:  22,
						BoolField:   false,
					},
				},
			},
			new: func() proto.Message {
				return new(pb.MessageWithRepeatedSubMessage)
			},
		},
		{
			name: "message with oneof",
			pb: &pb.MessageWithOneof{
				StringField: "baz",
				OneofField:  &pb.MessageWithOneof_Int32OneofField{3132},
			},
			new: func() proto.Message {
				return new(pb.MessageWithOneof)
			},
		},
	}
)

func TestMarshalUnmarshal(t *testing.T) {
	rb := bson.NewRegistryBuilder()
	rb.RegisterCodec(reflect.TypeOf((*proto.Message)(nil)).Elem(), NewProtobufCodec())
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
	rb.RegisterCodec(reflect.TypeOf((*proto.Message)(nil)).Elem(), NewProtobufCodec())
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
