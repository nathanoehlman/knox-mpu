## knox-mpu

A Node.js client designed to make large file uploads to Amazon S3 via the [MultiPartUpload API](http://docs.amazonwebservices.com/AmazonS3/latest/dev/sdksupportformpu.html) simple and easy. It's built on top of the excellent [Knox](https://github.com/LearnBoost/knox) library from the guys over at LearnBoost.

### Features

* Simple and easy to use
* Pipe either a file, or a stream directly to S3 (No need to know the content length first!)
* Automatically separates a file/stream into appropriate sized segments for upload
* Asynchronous uploading of segments
* Handy events to track your upload progress

_Planned_

* Better error handling (reuploading failed parts, etc)

### Installing

Installation is done via NPM, by running ```npm install knox-mpu```

### Examples

#### Uploading a stream

To upload a stream, simply pass the stream when constructing the MultiPartUpload. The upload will then listen to the stream, and create parts from incoming data stream. When a part reaches the minimum part size, it will attempt to upload it to S3.

```javascript

// Create a Knox client first
var client = knox.createClient({ ... }),
    upload = null;


upload = new MultiPartUpload(
            {
                client: client,
                objectName: 'destination.txt', // Amazon S3 object name
                stream: stream
            },
            // Callback handler
            function(err, body) {
                // If successful, will return body, containing Location, Bucket, Key, ETag and size of the object
                /*
                  {
                      Location: 'http://Example-Bucket.s3.amazonaws.com/destination.txt',
                      Bucket: 'Example-Bucket',
                      Key: 'destination.txt',
                      ETag: '"3858f62230ac3c915f300c664312c11f-9"',
                      size: 7242880
                  }
                */
            }
        );
````

#### Uploading a file

To upload a file, pass the path to the file in the constructor. Knox-mpu will split the file into parts and upload them.

```javascript

// Create a Knox client first
var client = knox.createClient({ ... }),
    upload = null;


upload = new MultiPartUpload(
            {
                client: client,
                objectName: 'destination.txt', // Amazon S3 object name
                file: ... // path to the file
            },
            // Callback handler
            function(err, body) {
                // If successful, will return body, containing Location, Bucket, Key, ETag and size of the object
                /*
                  {
                      Location: 'http://Example-Bucket.s3.amazonaws.com/destination.txt',
                      Bucket: 'Example-Bucket',
                      Key: 'destination.txt',
                      ETag: '"3858f62230ac3c915f300c664312c11f-9"',
                      size: 7242880
                  }
                */
            }
        );
````
### Options

The following options can be passed to the MultiPartUpload constructor -

* ```client``` _Required_ The knox client to use for this upload request
* ```objectName``` _Required_ The destination object name/path on S3 for this upload
* ```stream``` The stream to upload (required if file is not being supplied)
* ```file``` The path to the file (required if stream is not being supplied)
* ```headers``` Any additional headers to include on the requests
* ```partSize``` The minimum size of the parts to upload (default to 5MB).

### Events

The MultiPartUpload will emit a number of events -

* ```initiated``` Emitted when the multi part upload has been initiated, and received an upload ID. Passes the upload id through as the first argument to the event
* ```uploading``` Emitted each time a part starts uploading. The part id is passed as the first argument.
* ```uploaded``` Emitted each time a part finishes uploading. Passes through an object containing the part id and Amazon ETag for the uploaded part.
* ```error``` Emitted each time a part upload fails. Passes an object containing the part id and error message
* ```completed``` Emitted when the upload has completed successfully. Contains the object information from Amazon S3 (location, bucket, key and ETag)
