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
	// oneOfNamesCache map[string]map[string]bool
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
	props := proto.GetProperties(val.Type())
	indexedProps := make(map[string]*proto.Properties)
	indexedGoOneofWrapperProps := make([]*proto.Properties, 0)
	indexedOneofProps := make(map[string]*proto.OneofProperties)
	for _, prop := range props.Prop {
		if ph.isOneOf(prop) {
			indexedGoOneofWrapperProps = append(indexedGoOneofWrapperProps, prop)
		} else {
			indexedProps[strconv.Itoa(prop.Tag)] = prop
		}
	}
	for _, oneof := range props.OneofTypes {
		indexedOneofProps[strconv.Itoa(oneof.Prop.Tag)] = oneof
	}

	for _, prop := range indexedProps {
		fVal := val.FieldByName(prop.Name)
		if !fVal.IsZero() {
			// Figure out what tag and what value we are encoding.
			tag := prop.Tag

			// Actually encode the tag/value.
			fvw, err := dw.WriteDocumentElement(TagToElementName(tag))
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

	for _, prop := range indexedGoOneofWrapperProps {
		fVal := val.FieldByName(prop.Name)
		if !fVal.IsZero() {
			// If this field is a oneof, we need to get the single Go value stored inside its oneof struct,
			// instead of simply using the value as-is.
			oneof := fVal.Elem().Elem()
			singleProp := proto.GetProperties(oneof.Type()).Prop[0]
			tag := singleProp.Tag
			fVal = oneof.Field(0)

			// Actually encode the tag/value.
			fvw, err := dw.WriteDocumentElement(TagToElementName(tag))
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
	props := proto.GetProperties(val.Type())
	indexedProps := make(map[string]*proto.Properties)
	indexedGoOneofWrapperProps := make([]*proto.Properties, 0)
	indexedOneofProps := make(map[string]*proto.OneofProperties)
	for _, prop := range props.Prop {
		if ph.isOneOf(prop) {
			indexedGoOneofWrapperProps = append(indexedGoOneofWrapperProps, prop)
		} else {
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

		tag := elementNameToTag(f)
		prop, isProp := indexedProps[tag]
		oneof, isOneof := indexedOneofProps[tag]

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

func (pc *protobufCodec) protoHelper(pb descriptor.Message, t reflect.Type) *protoHelper {
	messageName := proto.MessageName(pb)
	if ph, ok := pc.protoHelpers[messageName]; ok {
		return ph
	}
	oneofNames := oneofNames(pb)

	props := proto.GetProperties(t)
	indexedProps := make(map[string]*proto.Properties)
	indexedGoOneofWrapperProps := make([]*proto.Properties, 0)
	indexedOneofProps := make(map[string]*proto.OneofProperties)
	for _, prop := range props.Prop {
		if oneofNames[prop.Name] {
			indexedGoOneofWrapperProps = append(indexedGoOneofWrapperProps, prop)
		} else {
			indexedProps[strconv.Itoa(prop.Tag)] = prop
		}
	}
	for _, oneof := range props.OneofTypes {
		indexedOneofProps[strconv.Itoa(oneof.Prop.Tag)] = oneof
	}
	for _, oneof := range props.OneofTypes {
		indexedOneofProps[strconv.Itoa(oneof.Prop.Tag)] = oneof
	}
	ph := &protoHelper{oneofNames}
	return ph
}

func oneofNames(pb descriptor.Message) map[string]bool {
	_, msgDescriptor := descriptor.ForMessage(pb)
	names := make(map[string]bool)
	for _, oneof := range msgDescriptor.OneofDecl {
		names[*oneof.Name] = true
	}
	return names
}

type protoHelper struct {
	oneOfNames map[string]bool
}

func (ph *protoHelper) isOneOf(p *proto.Properties) bool {
	return ph.oneOfNames[p.OrigName]
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
