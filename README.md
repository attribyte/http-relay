# http-relay

A library for creating services that relay data from an arbitrary Supplier to an HTTP endpoint.
Failed messages are retried with [exponential back-off](https://en.wikipedia.org/wiki/Exponential_backoff),
up to a configurable limit.

