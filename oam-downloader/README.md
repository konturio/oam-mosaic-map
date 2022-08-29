# OpenAerialMap image downloader

This docker image downloads all images from OpenAerialMap s3 bucket to persistent volume using OAM API

docker with ubuntu linux
runs oam-downloader
docker compose sets env var for bbox
downloads images
deletes images where less than 3 bands

#
### docker-compose notes

```
    environment:
      - WORKDIR=images
      - BBOX=bbox=27.44704,53.83303,27.70474,53.97516
```

`WORKDIR` - used in oam-downloader.sh, same dir as in volumes
`BBOX` - used in oam-downloader.sh, IF EMPTY - downloading all images

#
```
    volumes:
      - .\oam_images:/home/oam-downloader/images
```

`.\oam_images` - directory on localhost on in k8
