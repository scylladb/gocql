# Compression

Set `config.Compression` to `NoCompression`, `LZ4Compression` or
`SnappyCompression`. Implementations belong to the single v2 module; no
standalone compressor module or `replace` directive is needed. Native protocol
5 supports LZ4 segment compression. The outbound data path compresses encoded
spans directly and sends original spans when a v5 segment is incompressible.

`RunStream` supports no compression on raw/TLS transports and protocol 5 LZ4
segments. Protocol 3/4 frame compression with LZ4 or Snappy produces an explicit
`StreamCapabilityError` before streaming submission. There is no buffered
fallback. Normal buffered execution supports protocol 3/4 frame compression.

See [streaming](../statements/streaming.md) for transform staging and lifetime.
