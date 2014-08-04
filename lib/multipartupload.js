var _ = require('lodash'),
    EventEmitter = require('events').EventEmitter,
    async = require("async"),
    fs = require('fs'),
    path = require('path'),
    fsx = require('fs-extra'),
    os = require('os'),
    util = require('util'),
    parse = require('./parse');

/**
 * Initializes a Amazon S3 Multi part file upload with the given options
 */
function MultiPartUpload(opts, callback) {
    var e;

    if (!opts.client || !opts.objectName) {
      e = new Error('MultiPart upload must be created from a client and provide an object name');
      if (callback) return callback(e);
      throw e;
    }

    if (!opts.stream && !opts.file) {
      e = new Error('MultiPart upload must be passed either a stream or file parameter');
      if (callback) return callback(e);
      throw e;
    }

    if (opts.stream && opts.file) {
      e = new Error('You cannot provide both a stream and a file to upload');
      if (callback) return callback(e);
      throw e;
    }

    if (opts.noDisk && opts.partSize && opts.partSize > 10485760) {
      e = new Error('Keep in-memory part sizes 10MB or less');
      if (callback) return callback(e);
      throw e;
    }

    callback = callback || function(err, results) {};

    this.objectName = opts.objectName;
    this.fileName = opts.file;
    this.headers = opts.headers || {};
    this.client = opts.client;
    this.partSize = opts.partSize || 5242880; // 5MB default
    this.maxRetries = opts.maxRetries || 0;   // default to no retry
    this.uploadId = null;
    this.concurrency = opts.batchSize || 4;
    this.uploads = async.queue(function(task, callback) { task(callback); }, this.concurrency);
    this.noDisk = opts.noDisk;
    this.maxUploadSize = opts.maxUploadSize || 1/0; // infinity default
    this.currentUploadSize = 0;
    this.aborted = false;
    this.totalUploadSize = 0 || opts.totalUploadSize;
    this.fixedUploadSize = opts.totalUploadSize ? true : false;
    this.percentUploaded = 0;

    this.size = 0;
    this.parts = [];

    // initialise the tmp directory based on opts (fallback to os.tmpDir())
    this.tmpDir = !this.noDisk && (opts.tmpDir || os.tmpDir());

    // ensure the tmpdir exists
    fsx.ensureDir(this.tmpDir, function(err) {
      if (err) return callback(err);
      //dir has now been created, including the directory it is to be placed in
    });

    // If we're buffering to disk, double the "totalUploadSize" we expect since
    // every byte has to be uploaded twice (once to disk, once to S3)
    if (!this.noDisk) {this.totalUploadSize *= 2;}

    var mpu = this,
        written = 0;

    mpu.on('partProgress', function(data) {
        written += data.written;
        var percent = written / mpu.totalUploadSize * 100 | 0;
        mpu.percentUploaded = percent;
        mpu.emit('progress', {
            written: written,
            total: mpu.totalUploadSize,
            percent: mpu.percentUploaded
        });
    });

    // Recalculate progress as previously written data needs to be rewritten
    mpu.on('failed', function(part, partWritten) {
        written = written - partWritten;
    });

    if (opts.stream) {
        this._putStream(opts.stream, callback);
    } else {
        this._putFile(opts.file, callback);
    }

}
util.inherits(MultiPartUpload, EventEmitter);

/**
 * Attempts to initiate the MultiPartUpload request (gets the upload ID)
 */
MultiPartUpload.prototype._initiate = function(callback) {

    // Send the initiate request
    var req = this.client.request('POST', this.objectName + '?uploads', this.headers),
        mpu = this;

    // Handle the xml response
    parse.xmlResponse(req, function(err, body) {

        if (err) return callback(err);
        if (!body.UploadId) return callback('Invalid upload ID');

        mpu.uploadId = body.UploadId;
        mpu.emit('initiated', body.UploadId);
        return callback(null, body.UploadId);
    });

    req.end();
};

/**
 * Streams a file to S3 using a multipart form upload
 *
 * Divides the file into separate files, and then writes them to Amazon S3
 */
MultiPartUpload.prototype._putFile = function(file, callback) {
    if (!file) return callback('Invalid file');

    var mpu = this,
        parts = [];

    fs.exists(file, function(exists) {
        if (!exists) {
            return callback('File does not exist');
        }

        fs.lstat(file, function (err, stats) {
            var remainingBytes = stats.size;
            var offset = 0;
            mpu.totalUploadSize = mpu.totalUploadSize || stats.size;
            while (remainingBytes > mpu.partSize) {
                var partId = parts.length + 1,
                    part = {
                        id: partId,
                        fileName: mpu.fileName,
                        offset: offset,
                        length: mpu.partSize,
                        triesLeft: mpu.maxRetries + 1
                    };
                offset += mpu.partSize;
                remainingBytes -= mpu.partSize;
                parts.push(part);
                mpu.uploads.push(mpu._uploadPart.bind(mpu, part));
            }
            if (remainingBytes) {
                var partId = parts.length + 1,
                    part = {
                        id: partId,
                        fileName: mpu.fileName,
                        offset: offset,
                        length: remainingBytes,
                        triesLeft: mpu.maxRetries + 1
                    };
                parts.push(part);
                mpu.uploads.push(mpu._uploadPart.bind(mpu, part));
            }

            mpu._initiate(function(err, uploadId) {
                if (err || !uploadId) {
                    return callback('Unable to initiate file upload');
                }
                return mpu._completeUploads(callback);
            });
        });
   });
}

/**
 * Streams a stream to S3 using a multipart form upload.
 *
 * It will attempt to initialize the upload (if not already started), read the stream in,
 * write the stream to a temporary file of the given partSize, and then start uploading a part
 * each time a part is available
 */
MultiPartUpload.prototype._putStream = function(stream, callback) {

    if (!stream) return callback('Invalid stream');

    var mpu = this;

    if (!this.uploadId) {
        this._initiate(function(err, uploadId) {
            if (err || !uploadId) return callback('Unable to initiate stream upload [' + err || 'No upload ID' + ']');
        });
    }
    // Start handling the stream straight away
    mpu._handleStream(stream, callback);
};

/**
  Handles an incoming stream, divides it into parts, and uploads it to S3
 **/
MultiPartUpload.prototype._handleStream = function(stream, callback) {

    var mpu = this,
        parts = [],
        current;

    // Create a new part
    function newPart() {
        var partId = parts.length + 1,
            partFileName = path.resolve(path.join(mpu.tmpDir || '', ('mpu-' + mpu.objectName + '-' + random_seed() + '-' + (mpu.uploadId || Date.now()) + '-' + partId).replace(/\//g,''))),
            partFile = !mpu.noDisk && fs.createWriteStream(partFileName),
            part = {
                id: partId,
                stream: partFile,
                fileName: partFileName,
                offset: 0,
                length: 0,
                triesLeft: mpu.maxRetries + 1,
                data: Buffer('')
            };

        parts.push(part);
        return part;
    }

    function partReady(part) {
        if (!part) return;

        // Ensure the stream is closed
        if (part.stream && part.stream.writable) {
            part.stream.end();
        }
        mpu.uploads.push(mpu._uploadPart.bind(mpu, part));
    }

    function abortUpload(part) {
        // Ensure the stream is closed and temporary file removed
        if (part && part.stream.writable) {
            // Ensure the stream is closed
            if (part.stream.writable) {
                part.stream.end();
            }

            // Remove the temporary file
            fs.unlink(part.fileName, function(err) {
                if(err) return callback(err);
            });
        }

        current = null;
        mpu.aborted = true;
    }

    // Handle the data coming in
    stream.on('data', function(buffer) {
        // Check if we are over the max total limit
        if((mpu.currentUploadSize += buffer.length )> mpu.maxUploadSize){
            return abortUpload(current);
        }

        if (!current) {
            current = newPart();
        }

        if (current.stream) {
            current.stream.write(buffer);
            mpu.emit('partProgress', {
                written: buffer.length
            });
        } else {
            current.data = Buffer.concat([current.data, buffer]);
        }
        current.length += buffer.length;
        if (!mpu.fixedUploadSize) {
            mpu.totalUploadSize += buffer.length;
        }

        // Check if we have a part
        if (current.length >= mpu.partSize) {
            partReady(current);
            current = null;
        }
    });

    // Handle the end of the stream
    stream.on('end', function() {
        if (current) {
            partReady(current);
        }

        // Wait for the completion of the uploads
        if(mpu.aborted){
          return mpu._abortUploads(callback);
        }else{
          return mpu._completeUploads(callback);
        }
    });

    // Handle errors
    stream.on('error', function(err) {
        // Clean up
        return callback(err);
    });
};

/**
  Uploads a part, or if we are not ready yet, waits for the upload to be initiated
  and will then upload
 **/
MultiPartUpload.prototype._uploadPart = function(part, callback) {

    // If we haven't started the upload yet, wait for the initialization
    if (!this.uploadId) {
        return this.on('initiated', this._uploadPart.bind(this, part, callback));
    }

    var url = this.objectName + '?partNumber=' + part.id + '&uploadId=' + this.uploadId,
        headers = { 'Content-Length': part.length },
        req = this.client.request('PUT', url, headers),
        partStream = !this.noDisk && fs.createReadStream(part.fileName, {start: part.offset, end: part.offset + part.length - 1}),
        mpu = this,
        written = 0;

    // Wait for the upload to complete
    req.on('response', function(res) {
        // If we got a failure code and we haven't already called the callback for this part
        // (see https://github.com/nathanoehlman/knox-mpu/issues/27#issuecomment-45398796)
        // then handle the error appropriately
        if (res.statusCode != 200  && !part.alreadyCB) {
            var result = {part: part.id, message: 'Upload failed with status code '+res.statusCode };
            mpu.emit('failed', result);
            // If we haven't used up all of our retries, attempt to upload again
            if (--part.triesLeft) {
                return MultiPartUpload.prototype._uploadPart.call(mpu, part, callback);
            }
            // Otherwise bail out
            else {
                part.alreadyCB = true;
                return callback(result);
            }
        }

        // Grab the etag and return it
        var etag = res.headers.etag,
        result = { part: part.id, etag: etag };
        mpu.size += part.length;
        mpu.parts.push(result);

        mpu.emit('uploaded', result);

        // If we were saving to a temp file, delete it now
        if (!this.noDisk) {
            fs.unlink(part.fileName, function(err) {
                // Make sure the callback is only called once
                if (!part.alreadyCB){
                    part.alreadyCB = true;
                    return callback(err, result);
                }
            });
        } else if (!part.alreadyCB) {
            // Make sure the callback is only called once
            part.alreadyCB = true;
            return callback(null, result);
        }

    });

    // Handle errors
    req.on('error', function(err) {
        // If we already successfully saved this, ignore the error
        if (part && part.alreadyCB) {return;}
        var result = {part: part.id, message: err};
        mpu.emit('failed', result, written);
        if (--part.triesLeft) {
            return MultiPartUpload.prototype._uploadPart.call(mpu, part, callback);
        }
        else {
            // Make sure the callback is only called once
            part.alreadyCB = true;
            return callback(result);
        }
    });

    if (!this.noDisk) {
        partStream.on('data', function (data) {
            written += data.length;
            mpu.emit('partProgress', {
                part: part.id,
                written: data.length,
                overall: written,
                total: part.length,
                percent: written / part.length * 100 | 0
            });
        });
        partStream.pipe(req);
    } else {
        req.write(part.data);
        req.end();
    }

    mpu.emit('uploading', part.id);
};

/**
  Indicates that all uploads have been started and that we should wait for completion
 **/
MultiPartUpload.prototype._completeUploads = function(callback) {

    var mpu = this;

    this.uploads.drain = function() {

        var parts = _.sortBy(mpu.parts, function(value) { return value.part; });
        parts = _.map(parts, function(value) {
            return util.format('<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>', value.part, value.etag);
        }).join('');

        var req = mpu.client.request('POST', mpu.objectName + '?uploadId=' + mpu.uploadId);

        // Register the response handler
        parse.xmlResponse(req, function(err, body) {
            if (err) return callback(err);
            if (body === null) return callback('got empty response');
            delete body.$;
            body.size = mpu.size;
            mpu.emit('completed', body);
            return callback(null, body);
        });

        // Write the request
        req.write('<CompleteMultipartUpload>' + parts + '</CompleteMultipartUpload>');
        req.end();
    };
};

/**
  Indicates that upload has been aborted
 **/
MultiPartUpload.prototype._abortUploads = function(callback) {

    var mpu = this;

    this.uploads.drain = function() {

        var req = mpu.client.request('DELETE', mpu.objectName + '?uploadId=' + mpu.uploadId);

        // Register the response handler
        parse.xmlResponse(req, function(err, body) {
            if (err) return callback(err);
            return callback('reached maxUploadSize');
        });

        req.end();
    };
};

module.exports = MultiPartUpload;

function random_seed(){
    return 'xxxx'.replace(/[xy]/g, function(c) {var r = Math.random()*16|0,v=c=='x'?r:r&0x3|0x8;return v.toString(16);});
}
