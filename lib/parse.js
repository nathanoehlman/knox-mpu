var xml2js = require('xml2js');

/**
  Simple helper method to handle XML responses
 **/
exports.xmlResponse = function xmlResponse(req, callback) {
    
    if (!req) return callback('Invalid request');
    
    // Handle the response
    req.on('response', function(res) {
        var body = '';

        res.on('data', function(chunk){
            body += chunk;
        });

        res.on('end', function(){
            var parser = new xml2js.Parser({explicitArray: false, explicitRoot: false});            
            parser.parseString(body, callback);
        });

        res.on('error', callback);
    });
    
    req.on('error', callback);    
}