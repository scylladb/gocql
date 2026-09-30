# Changelog

## Unreleased

### Fixed

- Reject vector element lengths larger than the remaining payload before converting
  them to `int`. Malformed lengths now return an unexpected EOF error instead of
  overflowing and decoding as empty values on 32-bit or 64-bit systems.
