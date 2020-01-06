package protomongo

import (
	"reflect"
	"strconv"

	"github.com/golang/protobuf/descriptor"
	"github.com/golang/protobuf/proto"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
)

// ProtobufCodec is a MongoDB codec. It encodes protobuf objects using the protobuf field numbers as document
// keys. This means that stored protobufs can survive normal protobuf definition changes, e.g. renaming a field.
type ProtobufCodec struct {
}

func (e *ProtobufCodec) EncodeValue(ectx bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	oneofs := oneofNames(val.Interface().(descriptor.Message))
	for val.Kind() != reflect.Struct {
		val = val.Elem()
	}
	props := proto.GetProperties(val.Type())

	dw, err := vw.WriteDocument()
	if err != nil {
		return err
	}

	for _, prop := range props.Prop {
		fVal := val.FieldByName(prop.Name)
		if !fVal.IsZero() {
			// Figure out what tag and what value we are encoding.
			tag := prop.Tag
			if oneofs[prop.OrigName] {
				// If this field is a oneof, we need to get the single Go value stored inside its oneof struct,
				// instead of simply using the value as-is.
				oneof := fVal.Elem().Elem()
				singleProp := proto.GetProperties(oneof.Type()).Prop[0]
				tag = singleProp.Tag
				fVal = oneof.Field(0)
			}

			// Actually encode the tag/value.
			fvw, err := dw.WriteDocumentElement(strconv.Itoa(tag))
			if err != nil {
				return err
			}
			enc, err := ectx.LookupEncoder(fVal.Type())
			if err != nil {
				return err
			}
			if err = enc.EncodeValue(ectx, fvw, fVal); err != nil {
				return err
			}
		}
	}

	return dw.WriteDocumentEnd()
}

func (e *ProtobufCodec) DecodeValue(ectx bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	oneofs := oneofNames(val.Interface().(descriptor.Message))
	if val.IsNil() {
		val.Set(reflect.New(val.Type().Elem()))
	}
	for val.Kind() != reflect.Struct {
		val = val.Elem()
	}
	props := proto.GetProperties(val.Type())

	dr, err := vr.ReadDocument()
	if err != nil {
		return err
	}

	indexedProps := make(map[string]*proto.Properties)
	indexedOneofProps := make(map[string]*proto.OneofProperties)
	for _, prop := range props.Prop {
		if !oneofs[prop.OrigName] {
			indexedProps[strconv.Itoa(prop.Tag)] = prop
		}
	}
	for _, oneof := range props.OneofTypes {
		indexedOneofProps[strconv.Itoa(oneof.Prop.Tag)] = oneof
	}

	for f, fvr, err := dr.ReadElement(); err != bsonrw.ErrEOD; f, fvr, err = dr.ReadElement() {
		if err != nil {
			return err
		}

		prop, isProp := indexedProps[f]
		oneof, isOneof := indexedOneofProps[f]

		// Skip any field that we don't recognize.
		if !isProp && !isOneof {
			if err = vr.Skip(); err != nil {
				return err
			}
			continue
		}

		// Figure out what field we need to decode into.
		var fVal reflect.Value
		if isProp {
			fVal = val.FieldByName(prop.Name)
		} else if isOneof {
			oneofVal := reflect.New(oneof.Type.Elem())
			val.Field(oneof.Field).Set(oneofVal)
			fVal = oneofVal.Elem().Field(0)
		}

		// Actually decode the value.
		enc, err := ectx.LookupDecoder(fVal.Type())
		if err != nil {
			return err
		}
		if err = enc.DecodeValue(ectx, fvr, fVal); err != nil {
			return err
		}
	}

	return nil
}

func oneofNames(pb descriptor.Message) map[string]bool {
	_, msgDescriptor := descriptor.ForMessage(pb)
	names := make(map[string]bool)
	for _, oneof := range msgDescriptor.OneofDecl {
		names[*oneof.Name] = true
	}
	return names
}
