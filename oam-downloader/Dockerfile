FROM ubuntu

RUN mkdir -p /home/oam-downloader
RUN mkdir -p /home/oam-downloader/images

WORKDIR /home/oam-downloader

COPY Makefile .

RUN apt-get update && apt-get install -y make jq curl parallel wget

CMD ["make", "all"]