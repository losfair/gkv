# gkv

gkv is a geo-replicated eventually-consistent KV store built on FoundationDB. The core mechanism is active anti-entropy through Merkle trees.

The engine (gkvmesh) is written in Scala, and exposes a simple HTTP API for clients to use.

## Architecture

- **Cluster**: A gkv *cluster* is composed of a FoundationDB cluster and multiple gkvmesh instances. Each cluster has a full copy of data.
- **Mesh**: A *mesh* is a group of multiple clusters. They gossip with each other to keep their data eventually consistent.

## API

- `/data/get`

Get the value of a key.

Usage: `curl localhost:6300/data/get -d '{"key":"base64-encoded-key"}'`

- `/data/set`

Set the value of a key, or delete a key.

Usage (set): `curl localhost:6300/data/set -d '{"key":"base64-encoded-key","value":"base64-encoded-value"}'`

Usage (delete): `curl localhost:6300/data/set -d '{"key":"base64-encoded-key","delete":true}'`

- `/data/list`

List key-value pairs in a range.

Usage: `curl localhost:6300/data/list -d '{"start":"base64-encoded-start-key","end":"base64-encoded-end-key","limit":10}'`

- `/control/aae/pull`

Manually initiate an AAE (Active Anti-Entropy) pull. Usually used to bootstrap a new node.

Usage: `curl -i localhost:6302/control/aae/pull -d '{"peer":"localhost:6201"}'`
