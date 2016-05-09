![Relay](https://raw.githubusercontent.com/attribyte/http-relay/master/img/relay.png)

##About

A utility for creating services that relay data from an arbitrary supplier to an HTTP endpoint.
Messages are sent asynchronously with configurable concurrency. Any failed messages are retried 
with [exponential back-off](https://en.wikipedia.org/wiki/Exponential_backoff),
up to a configurable limit. All components are instrumented and reporting to [Graphite](http://graphite.wikidot.com/),
[CloudWatch](https://aws.amazon.com/cloudwatch/), [Essem](https://github.com/attribyte/essem), 
or [New Relic](http://newrelic.com/) is supported out-of-the box.

A service is built by configuring a component chain: 
[Supplier](https://attribyte.org/projects/http-relay/javadoc/org/attribyte/relay/Supplier.html) -> [Transformer](https://attribyte.org/projects/http-relay/javadoc/org/attribyte/relay/Transformer.html) -> [Relay](https://attribyte.org/projects/http-relay/javadoc/org/attribyte/relay/Relay.html) -> HTTP Endpoint.
You provide the Supplier, an optional Transformer and the HTTP endpoint configuration. Relay executes the chain
until shutdown or the Supplier is complete, retrying failed messages if necessary. Along the way, state may
be saved to allow the service to start up where it left off after shutdown.

##Documentation

[JavaDoc](https://www.attribyte.org/projects/http-relay/javadoc/index.html)