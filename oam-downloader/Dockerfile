FROM alpine

RUN mkdir -p /home/oam-downloader
RUN mkdir -p /home/oam-downloader/images

WORKDIR /home/oam-downloader

COPY Makefile .

RUN apk add --no-cache make jq curl parallel

CMD ["make", "all"]