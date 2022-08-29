FROM ubuntu

RUN mkdir -p /home/oam-downloader
RUN mkdir -p /home/oam-downloader/images

WORKDIR /home/oam-downloader

COPY oam-downloader.sh .
COPY del-if-one-band.sh .

RUN apt-get update && apt-get install -y jq curl parallel wget gdal-bin

CMD ["./oam-downloader.sh"]