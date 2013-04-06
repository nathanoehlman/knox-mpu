var _ = require('lodash'),
    EventEmitter = require('events').EventEmitter,
    Batch = require('batches'),
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

    if (opts.noDisk && opts.partSize && opts.partSize > 10485760) {
        throw new Error('Keep in-memory part sizes 10MB or less');
    }

    callback = callback || function(err, results) {};

    this.objectName = opts.objectName;
    this.headers = opts.headers || {};
    this.client = opts.client;
    this.partSize = opts.partSize || 5242880; // 5MB default
    this.uploadId = null;
    this.uploads = new Batch();
    this.noDisk = opts.noDisk;
    this.maxUploadSize = opts.maxUploadSize || 1/0; // infinity default
    this.currentUploadSize = 0;
    this.aborted = false;

    if( opts.batchSize ){
    	this.uploads.concurrency( opts.batchSize );
    }
    this.noDisk = opts.noDisk; // if true, uses in-memory data property of the part instead of a temp file

    // initialise the tmp directory based on opts (fallback to os.tmpDir())
    this.tmpDir = !this.noDisk && (opts.tmpDir || os.tmpDir());

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

    var mpu = this;

    fs.exists(file, function(exists) {
        if (!exists) {
            return callback('File does not exist');
        }

        var stream = fs.createReadStream(file);
        mpu._putStream(stream, callback);
    });
};

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
            partFileName = path.resolve(path.join(mpu.tmpDir || '', 'mpu-' + this.objectName + '-' + random_seed() + '-' + (mpu.uploadId || Date.now()) + '-' + partId)),
            partFile = !mpu.noDisk && fs.createWriteStream(partFileName),
            part = {
                id: partId,
                stream: partFile,
                fileName: partFileName,
                length: 0,
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
        } else {
            current.data = Buffer.concat([current.data, buffer]);
        }
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
        partStream = !this.noDisk && fs.createReadStream(part.fileName),
        mpu = this;

    // Wait for the upload to complete
    req.on('response', function(res) {
        if (res.statusCode != 200) return callback({part: part.id, message: 'Upload failed with status code '+res.statusCode});
        // Grab the etag and return it
        var etag = res.headers.etag,
            result = {part: part.id, etag: etag, size: part.length};

        mpu.emit('uploaded', result);

        // Remove the temporary file
        if (!mpu.noDisk) {
            fs.unlink(part.fileName, function(err) {
                return callback(err, result);
            });
        } else {
            return callback(null, result);
        }

    });

    // Handle errors
    req.on('error', function(err) {
        var result = {part: part.id, message: err};
        mpu.emit('failed', result);
        return callback(result);
    });

    if (!this.noDisk) {
        partStream.on('data', function (data) {
            mpu.emit('progress', {
                part: part.id,
                written: data.length,
                total: part.length,
                percent: data.length / part.length * 100 | 0
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

    this.uploads.end(function(err, results) {

        if (err) return callback(err);
        var size = 0, parts;
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

/**
  Indicates that all uploads have been started and that we should wait for completion
 **/
MultiPartUpload.prototype._abortUploads = function(callback) {

    var mpu = this;

    this.uploads.end(function(err, results) {

        if (err) return callback(err);

        var req = mpu.client.request('DELETE', mpu.objectName + '?uploadId=' + mpu.uploadId);

        // Register the response handler
        parse.xmlResponse(req, function(err, body) {
            if (err) return callback(err);
            return callback('reached maxUploadSize');
        });

        req.end();
    });
};

module.exports = MultiPartUpload;

function random_seed(){
    return 'xxxx'.replace(/[xy]/g, function(c) {var r = Math.random()*16|0,v=c=='x'?r:r&0x3|0x8;return v.toString(16);});
}
