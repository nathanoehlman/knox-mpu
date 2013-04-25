var _ = require('lodash'),
    EventEmitter = require('events').EventEmitter,
    Batch = require('batch'),
    fs = require('fs'),
    path = require('path'),
    os = require('os'),
    util = require('util'),
    parse = require('./parse');

/**
 * Initializes a Amazon S3 Multi part file upload with the given options
 */
function MultiPartUpload(opts, callback) {
    if (!opts.client || !opts.objectName) {
        throw new Error('MultiPart upload must be created from a client and provide a object name');
    }
    
    if (!opts.stream && !opts.file) {
        throw new Error('MultiPart upload must be passed either a stream or file parameter');
    }
    
    if (opts.stream && opts.file) {
        throw new Error('You cannot provide both a stream and a file to upload');
    }
 
    this.objectName = opts.objectName;
    this.fileName = opts.file;
    this.headers = opts.headers || {};
    this.client = opts.client;
    this.partSize = opts.partSize || 5242880; // 5MB default
    this.maxRetries = opts.maxRetries || 0;   // default to no retry
    this.uploadId = null;
    this.uploads = new Batch();
    this.uploads.concurrency(opts.concurrency || 16);

    // initialise the tmp directory based on opts (fallback to os.tmpDir())
    this.tmpDir = opts.tmpDir || os.tmpDir();
    
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
            if (err || !uploadId) return callback('Unable to initiate stream upload');
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
            partFileName = path.resolve(path.join(mpu.tmpDir, 'mpu-' + this.objectName + '-' + random_seed() + '-' + (mpu.uploadId || Date.now()) + '-' + partId)),
            partFile = fs.createWriteStream(partFileName),
            part = {
                id: partId,
                stream: partFile,
                fileName: partFileName,
                offset: 0,
                length: 0,
                triesLeft: mpu.maxRetries + 1
            };

        parts.push(part);
        return part;
    }

    function partReady(part) {
        if (!part) return;

        // Ensure the stream is closed
        if (part.stream.writable) {
            part.stream.end();
        }
        mpu.uploads.push(mpu._uploadPart.bind(mpu, part));
    }

    // Handle the data coming in
    stream.on('data', function(buffer) {
        if (!current) {
            current = newPart();
        }

        current.stream.write(buffer);
        current.length += buffer.length;

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
        return mpu._completeUploads(callback);
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
        partStream = fs.createReadStream(part.fileName, {start: part.offset, end: part.offset + part.length - 1}),
        mpu = this;

    // Wait for the upload to complete
    req.on('response', function(res) {
        if (res.statusCode != 200) {
            var result = {part: part.id, message: 'Upload failed'};
            mpu.emit('failed', result);
            if (--part.triesLeft)
                return MultiPartUpload.prototype._uploadPart.call(mpu, part, callback);
            else
                return callback(result);
        }
        
        // Grab the etag and return it
        var etag = res.headers.etag,
            result = {part: part.id, etag: etag, size: part.length};
        
        mpu.emit('uploaded', result);
        return callback(null, result);
    });
    
    // Handle errors
    req.on('error', function(err) {
        var result = {part: part.id, message: err};
        mpu.emit('failed', result);
        if (--part.triesLeft)
            return MultiPartUpload.prototype._uploadPart.call(mpu, part, callback);
        else
            return callback(result);
    });
      
    partStream.pipe(req);
    mpu.emit('uploading', part.id);
};

/**
  Indicates that all uploads have been started and that we should wait for completion
 **/
MultiPartUpload.prototype._completeUploads = function(callback) {    
    
    var mpu = this;    

    this.uploads.end(function(err, results) {
        
        if (err) return callback(err);
        
        var size = 0, parts, body;
        parts = _.map(results, function(value) {
            size += value.size;
            return util.format('<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>', value.part, value.etag);
        }).join('');
        
        var req = mpu.client.request('POST', mpu.objectName + '?uploadId=' + mpu.uploadId);
        
        // Register the response handler
        parse.xmlResponse(req, function(err, body) {
            if (err) return callback(err);        
            delete body.$;
            body.size = size;
            mpu.emit('completed', body);
            return callback(null, body);
        });
        
        // Write the request
        req.write('<CompleteMultipartUpload>' + parts + '</CompleteMultipartUpload>');
        req.end();
    });    
};

module.exports = MultiPartUpload;

function random_seed(){
    return 'xxxx'.replace(/[xy]/g, function(c) {var r = Math.random()*16|0,v=c=='x'?r:r&0x3|0x8;return v.toString(16);});
}
