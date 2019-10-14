package types

const (
	NilType    = "null"
	BoolType   = "boolean"
	BytesType  = "bytes"
	FloatType  = "float"
	DoubleType = "double"
	LongType   = "long"
	IntType    = "int"
	StringType = "string"
	RecordType = "record"

	// not supported yet:
	//	arrayType = "array"
	//	enumType  = "enum"
	//	fixedType = "fixed"
	//	mapType   = "map"

	TimestampMillis = "timestamp-millis"
	TimestampMicros = "timestamp-micros"
)
