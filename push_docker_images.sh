#!/bin/bash

set -e
# echo "PLEASE NOTE: you need to docker login with your own credentials into Kontur's nexus to push docker image"
(cd oam-downloader && docker build -t oam-downloader .)
(cd tiler && docker build -t raster-tiler .)
