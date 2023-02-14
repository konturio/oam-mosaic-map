function keyFromS3Url(url) {
  return url
    .replace("http://oin-hotosm.s3.amazonaws.com/", "")
    .replace("https://oin-hotosm.s3.amazonaws.com/", "")
    .replace("http://oin-hotosm-staging.s3.amazonaws.com/", "")
    .replace("https://oin-hotosm-staging.s3.amazonaws.com/", "")
    .replace(".tif", "");
}

export { keyFromS3Url };
