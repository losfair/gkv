#!/bin/bash

set -eo pipefail

id1="$(curl -f -s localhost:6301/cluster_id)"
id2="$(curl -f -s localhost:6302/cluster_id)"
id3="$(curl -f -s localhost:6303/cluster_id)"

echo "Cluster IDs: $id1 $id2 $id3"

export API_URL=http://localhost:6301

../set.sh "@system/peers/$id1" '{"address":"localhost:6201","upstreams":["localhost:6203"]}'
../set.sh "@system/peers/$id2" '{"address":"localhost:6202","upstreams":["localhost:6201"]}'
../set.sh "@system/peers/$id3" '{"address":"localhost:6203","upstreams":["localhost:6202"]}'

echo "Done."
