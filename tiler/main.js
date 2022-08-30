const config = require("./utils/config-helper").getConfig();
const express = require("express");
const morgan = require("morgan");
const cors = require("cors");
const landing = require("./routes/landing");
const footPrint = require("./routes/footPrint");
const oamTiles = require("./routes/oamTiles");
const reorTiles = require("./routes/reorTiles");
const health = require('@cloudnative/health-connect');
const live = require('./services/live');

const app = express();

let healthCheck = new health.HealthChecker();

//is db up
const livePromise = () => new Promise(async (resolve, _reject) => {

  const appFunctioning = await live.isLive();

  if (appFunctioning[0]) {
    resolve();
  } else {
    reject(new Error("App is not functioning correctly"));
  }

});

let liveCheck = new health.LivenessCheck("LivenessCheck", livePromise);
healthCheck.registerLivenessCheck(liveCheck);
//is application up
let readyCheck = new health.PingCheck("localhost", "", config.app.port);
healthCheck.registerReadinessCheck(readyCheck);

//healthcheck endpoints
app.use('/live', health.LivenessEndpoint(healthCheck));
app.use('/ready', health.ReadinessEndpoint(healthCheck));
app.use('/health', health.HealthEndpoint(healthCheck));

app.use(
  morgan(":method :url :status :res[content-length] - :response-time ms")
);
app.use(cors());

app.use("/", landing);
app.use("/outlines", footPrint);
app.use("/tiles", oamTiles);
app.use("/reor", reorTiles);

app.use((req, res, next) => {
  res.json(401, { "message": "Bad Request" })
});

app.use((error, req, res, next) => {
  res.json(500, { "message": "Something Went Wrong" });
});

process.on('uncaughtException', err => {
  console.log(err);
})

const start = async () => {
  console.log('>>>app started succesfully')
  try {
    app.listen(config.app.port);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
};

start();
