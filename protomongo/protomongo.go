package protomongo

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/golang/protobuf/descriptor"
	"github.com/golang/protobuf/proto"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
)

type protobufCodec struct {
	protoHelpers map[string]*protoHelper
}

// Returns a new instance of protobufCodec. protobufCodec is a MongoDB codec. It encodes protobuf objects using the protobuf
// field numbers as document keys. This means that stored protobufs can survive normal protobuf definition changes, e.g. renaming a field.
func NewProtobufCodec() *protobufCodec {
	return &protobufCodec{make(map[string]*protoHelper)}
}

func (pc *protobufCodec) EncodeValue(ectx bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	origAsDescMsg := val.Interface().(descriptor.Message)
	for val.Kind() != reflect.Struct {
		val = val.Elem()
	}

	dw, err := vw.WriteDocument()
	if err != nil {
		return err
	}

	ph := pc.protoHelper(origAsDescMsg, val.Type())

	for _, prop := range ph.normalPropsByTag {
		fVal := val.FieldByName(prop.Name)
		if !fVal.IsZero() {
			encodeField(ectx, dw, prop.Tag, fVal)
		}
	}

	for _, prop := range ph.oneofFieldWrapperProps {
		fVal := val.FieldByName(prop.Name)
		if !fVal.IsZero() {
			// Since this field is a oneof, we need to get the single Go value stored inside its oneof wrapper struct,
			// instead of simply using the value as-is.
			oneof := fVal.Elem().Elem()
			singleProp := proto.GetProperties(oneof.Type()).Prop[0]
			fVal = oneof.Field(0)
			encodeField(ectx, dw, singleProp.Tag, fVal)
		}
	}

	return dw.WriteDocumentEnd()
}

func encodeField(ectx bsoncodec.EncodeContext, dw bsonrw.DocumentWriter, tag int, fVal reflect.Value) error {
	fvw, err := dw.WriteDocumentElement(TagToElementName(tag))
	if err != nil {
		return err
	}
	enc, err := ectx.LookupEncoder(fVal.Type())
	if err != nil {
		return err
	}
	return enc.EncodeValue(ectx, fvw, fVal)
}

func (pc *protobufCodec) DecodeValue(ectx bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	origAsDescMsg := val.Interface().(descriptor.Message)
	if val.IsNil() {
		val.Set(reflect.New(val.Type().Elem()))
	}
	for val.Kind() != reflect.Struct {
		val = val.Elem()
	}

	dr, err := vr.ReadDocument()
	if err != nil {
		return err
	}

	ph := pc.protoHelper(origAsDescMsg, val.Type())
	for f, fvr, err := dr.ReadElement(); err != bsonrw.ErrEOD; f, fvr, err = dr.ReadElement() {
		if err != nil {
			return err
		}

		tag := elementNameToTag(f)
		prop, isProp := ph.normalPropsByTag[tag]
		oneof, isOneof := ph.oneofPropsByTag[tag]

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

type protoHelper struct {
	// Properties corresponding to 'normal' (non-oneof) protobuf fields.
	// Indexed by protobuf tag number (as a string).
	normalPropsByTag map[string]*proto.Properties
	// OneofProperties corresponding to oneof protobuf fields.
	// Indexed by protobuf tag number (as a string).
	oneofPropsByTag map[string]*proto.OneofProperties

	// Properties corresponding to Go wrapper types for protobuf oneof declarations.
	oneofFieldWrapperProps []*proto.Properties
}

func (pc *protobufCodec) protoHelper(pb descriptor.Message, t reflect.Type) *protoHelper {
	// Try to load a pre-existing protoHelper from cache, if it exists.
	messageName := proto.MessageName(pb)
	if ph, ok := pc.protoHelpers[messageName]; ok {
		return ph
	}

	// Find the names of all oneofs.
	_, msgDescriptor := descriptor.ForMessage(pb)
	oneofNames := make(map[string]bool)
	for _, oneof := range msgDescriptor.OneofDecl {
		oneofNames[*oneof.Name] = true
	}

	// Get the corresponding Go type's Properties, and divide them into three groups.
	// See comments on 'protoHelper' for details.
	props := proto.GetProperties(t)
	oneofFieldWrapperProps := make([]*proto.Properties, 0)
	normalPropsByTag := make(map[string]*proto.Properties)
	for _, prop := range props.Prop {
		if oneofNames[prop.OrigName] {
			oneofFieldWrapperProps = append(oneofFieldWrapperProps, prop)
		} else {
			normalPropsByTag[strconv.Itoa(prop.Tag)] = prop
		}
	}
	oneofPropsByTag := make(map[string]*proto.OneofProperties)
	for _, oneof := range props.OneofTypes {
		oneofPropsByTag[strconv.Itoa(oneof.Prop.Tag)] = oneof
	}
	ph := &protoHelper{normalPropsByTag, oneofPropsByTag, oneofFieldWrapperProps}
	pc.protoHelpers[messageName] = ph
	return ph
}

const (
	tagPrefix = "PBTag_"
)

func TagToElementName(tag int) string {
	return fmt.Sprintf("%v%v", tagPrefix, tag)
}

func elementNameToTag(elementName string) string {
	return strings.Replace(elementName, tagPrefix, "", 1)
}
