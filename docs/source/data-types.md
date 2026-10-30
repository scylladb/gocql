# Typed codecs

Use immutable `DataType` and `Columns` descriptors. `ScalarType`, `ListType`,
`SetType`, `MapType`, `TupleType`, `UDTType` and `VectorDataType` construct them.
`Encoder[T]`/`Decoder[T]` compile typed value plans; `ParamEncoder[P]` and
`RowDecoder[R]` compose statement plans. `Params(Param(...))` and
`Row(Column(...))` map struct fields explicitly.

Native scalars have explicit codecs such as Int32, Int64, Bool, UUIDCodec,
TimestampCodec and DurationCodec. `StringEncoder` encodes a Go string;
TextCodec decodes a borrowed Text. BytesEncoder accepts []byte and BlobCodec
uses borrowed Bytes. Collection decoders produce segmented borrowed views;
nullable/empty wrappers distinguish null, empty and present values. Unset is
an explicit binding state and is invalid in results.

`DynamicParams`/`DynamicRows`, `Value` and `EncodedValue` support metadata-driven
applications. Standalone `EncodeValueBorrowed`, `EncodeValueOwned` and
`DecodeValue` use the same codec implementation as execution. Explicit owned
conversion/Clone detaches payloads. Custom codecs use checked reader/writer
interfaces and preserve metadata validation and lifetime rules.

Streaming has separate `StreamDecoder` and `StreamRowDecoder` plans; these
incremental readers cannot call buffered decoders. See [streaming](statements/streaming.md).
