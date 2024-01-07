// Load the AWS SDK for Node.js
var AWS = require('aws-sdk');

// Required Libraries
var S3 = require('aws-sdk').S3, S3S = require('s3-streams');
const { GetObjectCommand, S3Client } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

// Configurations are inherited from EC2
require("dotenv").config();
AWS.config.update({
  aws_access_key_id: process.env.AWS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  aws_access_key_id: process.env.AWS_SESSION_TeOKEN,
  region: 'ap-southeast-2'
});
// Create the SQS and DynamoDB service object
var sqs = new AWS.SQS({ apiVersion: '2012-11-05' });
var ddb = new AWS.DynamoDB({ apiVersion: '2012-08-10' });

var queueURL = "queue_URL";
const ffmpeg = require('fluent-ffmpeg');

// Presigned url to retrieve S3 objects
const createPresignedUrlWithClient = ({ region, bucket, key }) => {
  const client = new S3Client({ region });
  const command = new GetObjectCommand({ Bucket: bucket, Key: key });
  return getSignedUrl(client, command, { expiresIn: 3600 });
};

// Recieve message from SQS parameters
var params = {
  AttributeNames: ["SentTimestamp"],
  MaxNumberOfMessages: 10,
  MessageAttributeNames: ["All"],
  QueueUrl: queueURL,
  VisibilityTimeout: 20,
  WaitTimeSeconds: 0,
};

function processMessages() {
  sqs.receiveMessage(params, function (err, data) {
    if (err) {
      console.log("Receive Error", err);
    } else if (data.Messages) {
      // Iterate through messages to apply transformation
      data.Messages.forEach(async function (message) {
        console.log("Received Message");
        // Transformation
        try {
          // Parse the message into JSON
          const messageData = JSON.parse(message.Body);
          const fileName = messageData.fileName;
          const selectedOutputFormat = messageData.selectedOutputFormat;
          const outputFileName = "transformed_" + fileName.split('.').slice(0, -1).join('.'); // Removes original file extension and adds 'transformed' to the name
          dbname = outputFileName + "." + selectedOutputFormat; // Set the output name with the new file type
          // Retrieve the original video from S3 using fileName from SQS
          const clientUrl = await createPresignedUrlWithClient({
            region: "ap-southeast-2",
            bucket: "bucket-name",
            key: fileName,
          });
          // The new S3 Link for the transformed file
          const newclientUrl = await createPresignedUrlWithClient({
            region: "ap-southeast-2",
            bucket: "bucket-name",
            key: dbname,
          });
          // S3Streams library to easily pipe video into S3, opposed to storing in local storage
          var upload = S3S.WriteStream(new S3(), {
            Bucket: 'bucket-name',
            Key: dbname,
            ContentType: 'application/octet-stream'
          });
          // Perform the transformation using ffmpeg
          ffmpeg()
            .input(clientUrl) // Inputs the video URL
            // Configuration for compression and downscale
            .format(selectedOutputFormat) // Set new output format 
            .outputOptions('-movflags frag_keyframe+empty_moov')
            .videoCodec("libx264")
            .videoBitrate("1000k")
            .size("1920x1080")
            .audioCodec("aac")
            .audioBitrate("128k")
            .on('end', function () { // Instructions for when transformation is completed
              console.log('Transformation complete');
              // Insert name and URL into DynamoDB
              var params = {
                TableName: 'table-name',
                Item: {
                  'username': { S: 'my-username' },
                  'TransformFile': { S: dbname },
                  'Link': { S: newclientUrl }
                }
              };
              ddb.putItem(params, function (err, data) {
                if (err) {
                  console.log("Error", err);
                } else {
                  console.log("Success", data);
                }
              });
            })
            .on('error', (err) => {
              // Insert Error for front end if errors occur
              console.error('An error occurred:', err);
              var params = {
                TableName: 'table-name',
                Item: {
                  'username': { S: 'my-username' },
                  'TransformFile': { S: dbname },
                  'Link': { S: "ERROR" }
                }
              };
              ddb.putItem(params, function (err, data) {
                if (err) {
                  console.log("Error", err);
                } else {
                  console.log("Success", data);
                }
                // Loop to continue processing messages in queue
                processMessages();
              });
            })
            .on('progress', function (progress) { // Show progress for debugging
              console.log('Processing: ' + progress.percent + '% done');
            })
            .on('codecData', function (data) {
              console.log('Input is ' + data.audio + ' audio ' +
                'with ' + data.video + ' video');
              console.log(clientUrl);
            })
            .pipe(upload, { end: true });
        }
        catch (error) {
          console.log('Error processing message:', error);
        };
        // Delete the SQS message
        var deleteParams = {
          QueueUrl: queueURL,
          ReceiptHandle: message.ReceiptHandle,
        };
        sqs.deleteMessage(deleteParams, function (err, deleteData) {
          if (err) {
            console.log("Delete Error", err);
          } else {
            console.log("Message Deleted", deleteData);
          }
        });
      });
    } else {
      // Keep waiting for new messages
      processMessages();
    }
  });
}

// Program run
processMessages();