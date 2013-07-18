var assert = require('assert'),
    fs = require('fs'),
    knox = require('knox'),
    os = require('os'),
    path = require('path'),
    MultiPartUpload = require('..'),
    mockstream = require('mockstream');

describe('Knox multipart form uploads', function() {

    var client = null;

    before(function(done) {
        try {
            var auth = require('./auth.json');
            client = knox.createClient(auth);
            done();
        } catch (err) {
            done('Could not create Knox client - please provide an ./auth.json file');
        }
    });

    it('should be able to pipe a stream directly to Amazon S3 using the multi part upload', function(done) {
        var testLength = 7242880,
            chunkSize = 2048,
            stream = new mockstream.MockDataStream({chunkSize: chunkSize, streamLength: testLength}),
            opts = {
                client: client, objectName: Date.now() + '.txt', stream: stream
            },
            mpu = null;
            
        // Upload the file
        mpu = new MultiPartUpload(opts, function(err, body) {
            if (err) return done(err);
            assert.equal(body['Key'], opts.objectName);
            
            // Clean up after ourselves
            client.deleteFile(opts.objectName, function(err, res) {
                if (err) return done('Could not delete file [' + err + ']');
                return done();
            });
            
        }); 
           
        stream.start(); 
    });

    it('should be able to abort a stream piped directly to Amazon S3 if max file using the multi part upload', function(done) {
        var testLength = 7242880,
            chunkSize = 2048,
            stream = new mockstream.MockDataStream({chunkSize: chunkSize, streamLength: testLength}),
            opts = {
                client: client, objectName: Date.now() + '.txt', stream: stream, maxUploadSize : testLength/2
            },
            mpu = null;

        // Upload the file
        mpu = new MultiPartUpload(opts, function(err, body) {
            assert.equal(err, "reached maxUploadSize");
            //Check that the file does not exist
            client.getFile(opts.objectName, function(err, res) {
                if (err) return done('Could not get file [' + err + ']');
                assert.equal(res.statusCode, 404);
                return done();
            });
        });

        stream.start();
    });

    it('should be able to upload a small file to S3', function(done) {
        
        var testLength = 242880,
            chunkSize = 2048,
            stream = new mockstream.MockDataStream({chunkSize: chunkSize, streamLength: testLength}),
            opts = {
                client: client, objectName: Date.now() + '.txt', stream: stream
            },
            mpu = null;
            
        // Upload the file
        mpu = new MultiPartUpload(opts, function(err, body) {
            if (err) return done(err);
            assert.equal(body['Key'], opts.objectName);
            
            // Clean up after ourselves
            client.deleteFile(opts.objectName, function(err, res) {
                if (err) return done('Could not delete file [' + err + ']');
                return done();
            });
            
        }); 
           
        stream.start();
        
    });

    it('should be able to upload a file to S3', function(done) {
        
        // Create a temporary file of data for uploading
        var tempFile = path.resolve(path.join(os.tmpDir(), 'knoxmpu-file-upload-test.txt')),
            writeStream = fs.createWriteStream(tempFile),
            mockDataStream = new mockstream.MockDataStream({chunkSize: 2048, streamLength: 6242880});            
        
        mockDataStream.on('data', function(chunk) {
            writeStream.write(chunk);
        });
        
        mockDataStream.on('end', function() {
            writeStream.end();
        });
        
        writeStream.on('error', done);
        mockDataStream.start();

        // Upload the file once we have a temporary file
        writeStream.on('close', function() {
            
            // Upload the file
            var opts = {
                    client: client, objectName: Date.now() + '.txt', file: tempFile
                },
                mpu = null;

            // Upload the file
            mpu = new MultiPartUpload(opts, function(err, body) {
                if (err) return done(err);
                assert.equal(body['Key'], opts.objectName);
                assert.equal(body.size, 6242880);

                // Clean up after ourselves
                client.deleteFile(opts.objectName, function(err, res) {                    
                    fs.unlink(tempFile, function(err2) {
                        return done((err || err2) ? 'Could not clean up after test' : null);
                    });
                });

            });            
        });
    });
})
