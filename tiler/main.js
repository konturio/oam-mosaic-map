const config = require("./utils/config-helper").getConfig();
const express = require("express");
const morgan = require("morgan");
const cors = require("cors");
const landing = require("./routes/landing");
const footPrint = require("./routes/footPrint");
const oamTiles = require("./routes/oamTiles");
const reorTiles = require("./routes/reorTiles");
const health = require('./routes/health');
const testtile = require('./routes/testtile');

const app = express();

app.use(
  morgan(":method :url :status :res[content-length] - :response-time ms")
);
app.use(cors());

app.use("/", landing);
app.use('/testtile', testtile);
app.use("/outlines", footPrint);
app.use("/tiles", oamTiles);
app.use("/reor", reorTiles);
//healthcheck endpoints
app.use('/health', health);

app.use((req, res, next) => {
  res.json(401, { "message": "Bad Request" })
});

app.use((error, req, res, next) => {
  console.log(error);
  res.json(500, { "message": "Something Went Wrong" });
});

process.on('uncaughtException', err => {
  console.log(err);
})

const start = async () => {
  try {
    const appPort = config.app.port;
    app.listen(appPort);
    console.log('>raster tiler server is listening on port ' + appPort);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
};

start();
