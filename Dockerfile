FROM node:18

WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install
COPY . .

EXPOSE 8080

CMD [ "node", "--max-old-space-size=4096", "src/main.mjs" ]
