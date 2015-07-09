console.log("Broker starting up...");

var mosca = require('mosca');
var request = require('request');

var mongoUrl = "mongodb://localhost:27017/mqtt";

var ascoltatore = { //Settings for ascoltatore backend. See: mongo_ascoltatore.js
    type: 'mongo',
    url: mongoUrl,
    pubsubCollection: 'ascoltatori',
    //maxRetry: 5, //appears to be max reconnection attempts, in case db disconnects, Default: 5
    //wait: 100,   //Something about database polling, Default: 100
    //size: 10 * 1024 * 1024, //Max size of ascoltatori collection. Default: 10MB.
    //max: 10000, //Maximum number of messages in ascoltatori history collection
    mongo: {} //mongo settings, passed in to MongoClient.connect(url, OPTS,cb);
};

var moscaSettings = {
    port: 1883,
    backend: ascoltatore,
    stats: false, //publish stats every 10 sec (default: false)
    //maxInflightMessages: //the maximum number of inflight messages per client.
    persistence: {
        factory: mosca.persistence.Mongo,
        url: mongoUrl,
        ttl: {
            subscriptions:60*60*1000, //the time (ms) after which subscriptions will expire. Default 1 hour.
            packets:      60*60*1000  //the time (ms) after which packets will expire. Default 1 hour.
        }//,
        //mongo: {},
        //connection: //a mongo client to be reused
        //storeMessagesQos0: false //like mosquitto option 'queue_qos0_messages', Default=false
    }
    //secure:{
    //     port: //port used to open the secure server
    //     keyPath: //path to the key
    //     certPath: //path to the certificate
    // }
    //allowNonSecure: false, //starts both secure & insecure server
    //http: {
    //    port,  //port used to open the http server
    //    bundle,//serve the bundled mqtt client
    //    static //serve a directory
    // }
};

//--------------------------------------------------------------------------------
var server = new mosca.Server( moscaSettings );

server.once('ready', function(){
    server.authenticate = function(client, username, password, callback){
        if(!client) return callback("No client!", false);
        username = username || "";
        password = password || "";

        var isAuthorized = (password.toString() === "secret");
        console.log("Authenticate [" + client.id + "]-(" + username +"/"+password+ ") => " + isAuthorized);
        return callback(null, isAuthorized);
    };

    server.authorizePublish = function(client, topic, payload, callback){
        var isAuthorized = true;
        console.log("Auth_Publish [" + client.id + "] => " + isAuthorized);
        return callback(null, isAuthorized); //authorize whoever, man
    };

    server.authorizeSubscribe = function(client, topic, callback){
        var topicRoot = topic.split('/')[0];
        var topicUser = topic.split('/')[1];
        var isAuthorized = true; //(topicRoot == "feeders" && client.user == topicUser);
        console.log("Auth_Subscribe [" + client.id + "] topic:("+topic+") => " + isAuthorized);
        return callback(null, isAuthorized);
    };

    console.log("Server ready!");
});

server.on('clientConnected', function(client) { //when a client is connected
    var date = new Date();
    console.log('__________________Connection: '+client.id+'_________________' + date.getDay() + "/" + date.getMonth() + "/" + date.getFullYear() + " @ " + date.getHours() + ":" + date.getMinutes() + ":" + date.getSeconds());
});

server.on('clientDisconnecting', function(client){
   console.log('Disconnecting: ' + client.id);
});

server.on('clientDisconnected', function(client) { //when a client is disconnected
    var date = new Date();
    console.log('DISCONNECTED: ' + client.id + ", Time: " + date.getDay() + "/" + date.getMonth() + "/" + date.getFullYear() + " @ " + date.getHours() + ":" + date.getMinutes() + ":" + date.getSeconds());
});

server.on('subscribed', function(topic, client){ //when a client is subscribed to a topic
    var date = new Date();
    console.log(client.id + " subscribed to topic: " + topic + ", Time: " + date.getDay() + "/" + date.getMonth() + "/" + date.getFullYear() + " @ " + date.getHours() + ":" + date.getMinutes() + ":" + date.getSeconds());
});

server.on('unsubscribed', function(topic, client){ //when a client is unsubscribed to a topic
    var date = new Date();
    console.log(client.id + " unsubscribed from topic: " + topic + ", Time: " + date.getDay() + "/" + date.getMonth() + "/" + date.getFullYear() + " @ " + date.getHours() + ":" + date.getMinutes() + ":" + date.getSeconds());
});

server.on('published', function(packet, client) { //when a new message is published
    var date = new Date();
    console.log('Published. Topic: ' +  packet.topic + ", Payload: " + packet.payload + ", Time: " + date.getDay() + "/" + date.getMonth() + "/" + date.getFullYear() + " @ " + date.getHours() + ":" + date.getMinutes() + ":" + date.getSeconds());
});

server.on('error', function(err){ //not sure if this is a real event???
    var date = new Date();
    console.error("Error: " + err + ", Time: " + date.getDay() + "/" + date.getMonth() + "/" + date.getFullYear() + " @ " + date.getHours() + ":" + date.getMinutes() + ":" + date.getSeconds());
});

