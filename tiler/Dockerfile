FROM node:18

# install gdal
RUN apt-get update
RUN apt-get install -y gdal-bin

# remove unneeded .deb files
RUN rm -r /var/lib/apt/lists/*

WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install
COPY . .

RUN mkdir /usr/src/app/tiles-cache
RUN mkdir /oam_src

EXPOSE 8080

CMD [ "node", "main.js" ]
