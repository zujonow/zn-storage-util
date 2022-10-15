const S3Lib = require("./lib/s3.lib");
const fs = require("fs");
const ffmpeg = require("fluent-ffmpeg");
const clientS3 = require("@aws-sdk/client-s3");
const { BlobServiceClient } = require("@azure/storage-blob");

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
    throw new Error(error);
  }
};

/**
 *
 * @param {Object} blobObject
 * @param {String} blobObject.filePath - absolute file path on your disk
 * @param {String} blobObject.blobPath - destination file path on azure blob
 * @param {Object} blobObject.blobServiceClient - azure blob client
 * @param {String} blobObject.containerName - azure blob container name
 *
 */

module.exports.uploadToBlob = async (blobObject) => {
  //
  const { filePath, blobPath, blobServiceClient, containerName } = blobObject;

  //
  if (!filePath) {
    throw new Error("filePath is required");
  }

  //
  if (!blobPath) {
    throw new Error("blobPath is required");
  }

  //
  if (!blobServiceClient) {
    throw new Error("BlobServiceClient is required");
  }

  //
  if (!containerName) {
    throw new Error("ContainerName is required");
  }

  //
  if (!fs.existsSync(filePath)) {
    throw new Error("file not found, please provide absolute file path");
  }

  //
  const containerClient = blobServiceClient.getContainerClient(containerName);

  //
  await containerClient.createIfNotExists({
    access: "container",
  });

  //
  const blockBlobClient = containerClient.getBlockBlobClient(blobPath);

  try {
    //
    if (fs.statSync(filePath).isDirectory()) {
      //
      throw new Error("Directory is not supported yet !!");
    } else {
      //
      await blockBlobClient.uploadFile(filePath);
    }
  } catch (error) {
    //
    throw new Error(error);
  }
};

/**
 *
 * @param {String} filePath
 * @returns {{width: number, height: number, duration: number, format: string, size: number}}
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

/**
 *
 * @param {String} filePath
 * @returns {{duration: number, format: string, size: number}}
 */
module.exports.getAudioFileMeta = async (filePath) => {
  return new Promise((resolve, reject) => {
    ffmpeg.ffprobe(filePath, async (err, metadata) => {
      if (err) return reject(err);
      resolve({
        duration: metadata.format.duration,
        format: metadata.format.format_name,
        size: metadata.format.size,
      });
    });
  });
};
/**
 *
 * @param {Object} awsObject
 * @param {String} awsObject.filePath - absolute file path on your disk
 * @param {Object} awsObject.s3Client - aws s3 client
 * @param {String} awsObject.awsBucket - aws s3 bucket name
 *
 */
module.exports.deleteFromS3 = async (awsObject) => {
  const { filePath, s3Client, awsBucket } = awsObject;

  const objectParams = {
    Bucket: awsBucket,
    Key: filePath,
  };

  if (filePath.includes(".")) {
    await s3Client.deleteObject(objectParams);
  } else {
    objectParams.Key += "/";
    await S3Lib.deleteDir(s3Client, objectParams);
  }
};

module.exports.clientS3 = clientS3;
module.exports.BlobServiceClient = BlobServiceClient;
