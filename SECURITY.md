# Security policy

http-over-kafka sits in the path of authenticated HTTP calls and decides whether a side effect runs. Security reports are taken seriously.

## Reporting a vulnerability

Please do not open a public issue. Report it privately through GitHub: on the repository page, go to **Security**, then **Report a vulnerability**. Include what you found, how to reproduce it, and the impact you expect.

You will get an acknowledgement, and we will keep you informed while a fix is prepared. We are happy to credit you in the release notes if you wish.

## Supported versions

The project is experimental and has no stable release yet. Fixes land on `main`.

## What counts as a vulnerability

In particular, anything that breaks one of these properties:

- a caller secret (`Authorization`, cookies, API keys, credentials declared in the service's OpenAPI) ends up in a Kafka record;
- a service is called more than once for the same request, outside the documented exceptions (`PUT`, `DELETE`, `x-conduktor-retry-safe`);
- a forged, replayed or moved record (command, response, result, state) is accepted;
- a caller receives a response that belongs to another request;
- a business event is published for something that did not happen.

## Known, documented limits

These are known and listed in the README; reports about them are still welcome if you see a way to make them worse:

- The keys in `deploy/*.env` are development keys, committed on purpose and named `dev-insecure-*`. Never use them outside a laptop.
- Anyone with write access to the internal topics can stop a service or suppress events. Signatures detect tampering; Kafka ACLs are what prevent it.
- Signing keys are per role, not per service.
