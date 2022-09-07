#!/bin/bash

set -e
echo "PLEASE NOTE: you need to docker login with your own credentials into Kontur's nexus to push docker image"

(cd tiler && docker build -t nexus.kontur.io:8085/konturdev/raster-tiler .)
docker push nexus.kontur.io:8085/konturdev/raster-tiler
