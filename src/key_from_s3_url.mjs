function keyFromS3Url(url) {
  const regex = /^https?:\/\/[^\/]+\.s3[^\/]*\.amazonaws\.com\/(.+)\.tif$/;

  const match = url.match(regex);
  return match ? match[1] : null;
}

export { keyFromS3Url };
