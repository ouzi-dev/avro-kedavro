package types

type FieldType int

const (
	Unknown   FieldType = 0
	Primitive FieldType = 1
	Union     FieldType = 2
)

type Options struct {
	IsStringToNumber    bool
	IsStringToBool      bool
	IsTimestampToMillis bool
	IsTimestampToMicros bool
	IsFormatDateTime    bool
	DateTimeFormat      string
}
