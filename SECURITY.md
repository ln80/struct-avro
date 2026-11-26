# Security policy

## Reporting a vulnerability

Report privately through GitHub:
[Report a vulnerability](https://github.com/ln80/struct-avro/security/advisories/new).
Do not open a public issue, pull request or discussion for a suspected
vulnerability.

Include the affected version, what an attacker gains, and a reproduction if
you have one. We aim to acknowledge a report within 7 days and to agree a
disclosure date with you once a fix is ready. Reporters are credited in the
advisory unless they ask not to be.

## Supported versions

Until v1, only the latest minor release receives security fixes.

## What the library guarantees

struct-avro generates Avro schemas from Go types, stores and resolves them,
and generates Go code from stored schemas. It guarantees that:

- decoding bytes through `registry.Registry`, or through an API from `NewAPI`,
  returns an error for malformed input and never panics. Allocation is bounded
  by `DefaultMaxSliceAllocSize` and `DefaultMaxByteSliceSize` unless the caller
  raises them;
- a schema or schema ID read from a store or from a payload is checked before
  use. An invalid one is an error, never a panic, and an ID never selects
  another schema;
- a field with a sensitive tag that cannot be parsed fails schema generation.
  It is never left out of the schema or its fingerprint;
- a stored schema version is never overwritten;
- generated Go code contains no text from a schema that could change its
  meaning: a doc with a line break or a tag value that leaves its struct tag
  fails generation.

A breach of one of these is a vulnerability.

## Out of scope

- The store is trusted to return the schema it stored. A store that serves a
  different but valid schema changes how data decodes.
- Field annotations are metadata. The library does not encrypt or redact the
  values of sensitive fields.
- Vulnerabilities in the Go toolchain or dependencies, unless this module
  exposes them in a way their maintainers would not consider a bug of theirs.
