const express = require("express");
const router = express.Router();
const fs = require("fs");
const { spawn } = require('child_process');
const path = require('path');

router.get("/", async (req, res) => {

  try {
    //change with your own folder
    const tileFilePath = `/Users/saricicek/Documents/Kontur/mapproxy/mytile.png`;
    if (!fs.existsSync(tileFilePath)) {
      const gdalwarp = spawn("gdalwarp", [
        //"-srcnodata",
        //0,
        "-t_srs",
        "epsg:4326",
        "-dstalpha",
        "-ts",
        256,
        256,
        "-te",
        27.624053,
        53.953426,
        27.6254852,
        53.9546741,
        path.resolve(__dirname, "../5.tif"),
        tileFilePath,
      ], {
        detached: true
      });
      gdalwarp.unref();

      gdalwarp.stdout.on('data', (data) => {
        //data started to be created
      });
      
      gdalwarp.stderr.on('data', (data) => {
        console.error(`stderr: ${data}`);
      });
      
      gdalwarp.on('close', (code) => {
        //spawn ended and closed
        fs.createReadStream(tileFilePath).pipe(res);
      });
    }
    else {
      fs.createReadStream(tileFilePath).pipe(res);
    }

  } catch (err) {
    console.log(err);
    throw err;
  } 
});

module.exports = router;
