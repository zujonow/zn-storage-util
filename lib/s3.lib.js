const fs = require("fs");
const path = require("path");

const S3Lib = {};

/** Upload directory to S3 */
S3Lib.uploadDir = async (s3Client, dirPath, { bucket, bucketPath, acl }) => {
  await recursiveWalk(dirPath, async (filePath) => {
    const params = {
      Bucket: bucket,
      Key: filePath.replace(dirPath, bucketPath),
      Body: fs.readFileSync(filePath),
      ACL: acl,
    };

    try {
      await s3Client.putObject(params);
    } catch (error) {
      console.error(error);
      throw new Error(`error in uploading ${filePath} to s3 bucket`);
    }
  });
};

/** Empty directory and then delete it from S3 */
S3Lib.deleteDir = async (s3Client, params) => {
  const result = { deletedCount: 0 };

  //
  const list = await s3Client.listObjects({
    Bucket: params.Bucket,
    Prefix: params.Key,
  });

  if (!list.Contents) return result;

  if (!list.Contents.length) {
    await s3Client.deleteObject({
      Bucket: params.Bucket,
      Key: params.Prefix,
    });
    // todo here we can check if directory id deleted.
    return result;
  }

  //
  const deleteParams = {
    Bucket: params.Bucket,
    Delete: { Objects: [] },
  };

  list.Contents.forEach(({ Key }) => {
    if (!Key) return null;

    deleteParams.Delete.Objects.push({ Key });
  });

  const deleteResponse = await s3Client.deleteObjects(deleteParams);
  result.deletedCount += deleteResponse.Deleted
    ? deleteResponse.Deleted.length
    : 0;

  const recursiveResult = await S3Lib.deleteDir(s3Client, params);
  result.deletedCount += recursiveResult.deletedCount;

  return result;
};

async function recursiveWalk(currentDirPath, callback) {
  const list = fs.readdirSync(currentDirPath);

  for (let i = 0; i < list.length; i++) {
    const filePath = path.join(currentDirPath, list[i]);
    const stat = fs.statSync(filePath);

    if (stat.isFile()) await callback(filePath, stat);

    if (stat.isDirectory()) await recursiveWalk(filePath, callback);
  }
}

module.exports = S3Lib;
