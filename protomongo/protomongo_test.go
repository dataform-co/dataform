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
		name          string
		pb            proto.Message
		equivalentPbs []proto.Message
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
			equivalentPbs: []proto.Message{
				&pb.RepeatedFieldMessage{
					StringField: []string{"foo"},
					Int32Field:  []int32{32525},
					Int64Field:  []int64{1531541553141312315},
					FloatField:  []float32{21541.3242},
					DoubleField: []float64{21535215136361617136.543858},
					BoolField:   []bool{true},
					EnumField:   []pb.Enum{pb.Enum_VAL_2},
				},
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
			equivalentPbs: []proto.Message{
				&pb.SimpleMessage{
					StringField: "bar",
					Int32Field:  435,
					Int64Field:  13512516266,
					FloatField:  3435.322,
					DoubleField: 213143343.76767,
					BoolField:   true,
					EnumField:   pb.Enum_VAL_1,
				},
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
			equivalentPbs: []proto.Message{
				&pb.MessageWithRepeatedSubMessage{
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
					},
				},
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
			equivalentPbs: []proto.Message{
				&pb.MessageWithSubMessage{
					StringField: "baz",
					SimpleMessage: &pb.SimpleMessage{
						StringField: "qux",
						Int32Field:  22,
						Int64Field:  1531541553141312315,
						FloatField:  21541.3242,
						DoubleField: 21535215136361617136.543858,
						// It might be expected that because the last element of the 'SimpleMessage' slice in 'pb' explicitly sets 'BoolField' to false,
						// this field should also be false, because the elements of the 'SimpleMessage' slice should be merged in order.
						// However, by the rules of proto3, default field values are never serialized. Thus when the second element
						// of the 'SimpleMessage' slice is deserialized, that deserialized value contains no value for 'BoolField', and thus
						// this field retains the value that was set in the first element of that slice.
						BoolField: true,
						EnumField: pb.Enum_VAL_2,
					},
				},
			},
		},
		{
			name: "message with oneof",
			pb: &pb.MessageWithOneof{
				StringField: "baz",
				OneofField:  &pb.MessageWithOneof_Int32OneofField{3132},
			},
			equivalentPbs: []proto.Message{},
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

		for _, equivalentPb := range append(testCase.equivalentPbs, testCase.pb) {
			out := reflect.New(reflect.TypeOf(equivalentPb).Elem()).Interface().(proto.Message)
			if err = bson.UnmarshalWithRegistry(reg, b, &out); err != nil {
				t.Errorf("bson.UnmarshalWithRegistry error = %v", err)
			}
			if !proto.Equal(equivalentPb, out) {
				t.Errorf("failed: in=%#q, out=%#q", equivalentPb, out)
			}
		}
	}
}

func TestMarshalUnmarshalWithPointers(t *testing.T) {
	rb := bson.NewRegistryBuilder()
	rb.RegisterCodec(reflect.TypeOf((*proto.Message)(nil)).Elem(), NewProtobufCodec())
	reg := rb.Build()

	for _, testCase := range tests {
		b, err := bson.MarshalWithRegistry(reg, testCase.pb)
		if err != nil {
			t.Errorf("bson.MarshalWithRegistry error = %v", err)
		}

		for _, equivalentPb := range append(testCase.equivalentPbs, testCase.pb) {
			out := reflect.New(reflect.TypeOf(equivalentPb).Elem()).Interface().(proto.Message)
			if err = bson.UnmarshalWithRegistry(reg, b, &out); err != nil {
				t.Errorf("bson.UnmarshalWithRegistry error = %v", err)
			}
			if !proto.Equal(equivalentPb, out) {
				t.Errorf("failed: in=%#q, out=%#q", equivalentPb, out)
			}
		}
	}
}
