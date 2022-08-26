FROM node:16

# install gdal
RUN apt-get update
RUN apt-get install -y gdal-bin

# remove unneeded .deb files
RUN rm -r /var/lib/apt/lists/*

WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install
COPY . .

EXPOSE 7802

CMD [ "node", "main.js" ]
