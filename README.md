# Mosaic Tiler

Mosaic Tiler for generating tiles from [OpenAerialMap.org](OpenAerialMap.org) images by Kontur
#
### This is nodejs server application.
#

## Algorithm

> {z,x,y} request -> query to __layers-db__ for list of images -> generating tile

Links to images and metadata about images stored in __layers-db__.

To generate tile out of couple images on the same teritory query filters images by ground resolution and latest uploaded date



## Running

There is docker image with tiler and `kube.yaml` configuration file to deploy in k8s.

Before running in k8s:
* set ENV vars in manifest 

    ```
    BASE_URL
    TILES_CACHE_DIR_PATH
    TMP_DIR_PATH

    PGHOST
    PGUSER
    ```

* create layer in __layers_db__

* insert metadata information as layer features in __layers_db__
