const S3Lib = require("./lib/s3.lib");
const fs = require("fs");
const ffmpeg = require("fluent-ffmpeg");
const clientS3 = require("@aws-sdk/client-s3");

/**
 *
 * @param {Object} awsObject
 * @param {String} awsObject.filePath - absolute file path on your disk
 * @param {String} awsObject.awsFilePath - destination file path on aws s3
 * @param {Object} awsObject.s3Client - aws s3 client
 * @param {String} awsObject.awsBucket - aws s3 bucket name
 * @param {String} awsObject.awsACL - aws s3 ACL
 *
 */
module.exports.uploadToS3 = async (awsObject) => {
  //
  const { filePath, awsFilePath, s3Client, awsBucket, awsACL } = awsObject;

  if (!filePath) {
    throw new Error("filePath is required");
  }

  if (!awsFilePath) {
    throw new Error("awsFilePath is required");
  }

  if (!s3Client) {
    throw new Error("s3Client is required");
  }

  if (!awsBucket) {
    throw new Error("awsBucket is required");
  }

  if (!awsACL) {
    throw new Error("awsACL is required");
  }

  if (!fs.existsSync(filePath)) {
    throw new Error("file not found, please provide absolute file path");
  }

  const objectParams = {
    Bucket: awsBucket,
    Key: awsFilePath,
    ACL: awsACL,
  };

  try {
    if (fs.statSync(filePath).isDirectory()) {
      await S3Lib.uploadDir(s3Client, filePath, {
        bucket: objectParams.Bucket,
        bucketPath: objectParams.Key,
        acl: objectParams.ACL,
      });
    } else {
      objectParams.Body = fs.createReadStream(filePath);
      await s3Client.putObject(objectParams);
    }
  } catch (error) {
    throw new Error(error.message);
  }
};

/**
 *
 * @param {String} filePath
 * @returns {{width: number, height: number, duration: number, format: string}}
 */
module.exports.getFileMeta = async (filePath) => {
  return new Promise((resolve, reject) => {
    ffmpeg.ffprobe(filePath, async (err, metadata) => {
      if (err) return reject(err);

      // get the max resolution video stream
      const streams = metadata.streams.filter((s) => s.codec_type === "video");

      if (streams.length == 0) {
        return reject(new Error("No video stream found"));
      }

      // get the max resolution video stream
      const vStream = streams.reduce((a, b) => {
        if (b.width > b.height) {
          return b.height > a.height ? b : a;
        } else {
          return b.width > a.width ? b : a;
        }
      });

      resolve({
        width: vStream.width,
        height: vStream.height,
        duration: metadata.format.duration,
        format: metadata.format.format_name,
        size: metadata.format.size,
      });
    });
  });
};

module.exports.clientS3 = clientS3;
