require("./environment").load();
const DEBUG = false;
const express = require('express');
const {processRequest} = require("../queue/jobCreationQueue");
const {INDEXER, JOBCREATOR} = require("../queue/queueConfigs");
const http = require('http');
const Arena = require('bull-arena');
const Bee = require("bee-queue");
require('../queue/jobCreationQueue');
const dgram = require('dgram');
const DEFAULT_QUERY = process.env.DEFAULT_QUERY || "_timestamp_:[NOW-1WEEK TO NOW]";
const HTTP_PORT = process.env.HTTP_PORT || 3000;
const UDP_PORT = process.env.UDP_PORT || 3001;
const ARENA_PORT = process.env.ARENA_PORT || 4567;
const KMAPS_LIST = process.env.KMAPS_LIST || [];
const TIMEBACK = "1MONTHS";  // How far back to index. See: https://solr.apache.org/guide/8_8/working-with-dates.html#date-math
// let filter_query = `_timestamp_:[NOW-${TIMEBACK} TO NOW]`;
// // filter_query = "projects_ss:*"; // Uncomment for custom queries
// filter_query = DEFAULT_QUERY || filter_query || generateKmapsQuery(KMAPS_LIST);

// Inits
const app = express();
// app.use(express.json());
const dgsocket = dgram.createSocket('udp4');

dgsocket.on('listening', () => {
    let addr = dgsocket.address();
    console.log(`UDP Listener started at ${addr.address}:${addr.port}`);
})

dgsocket.on('error', (err) => {
    console.error(`UDP error: ${err.stack}`);
});

dgsocket.on('connect', () => {
    console.error("UDP connect: ", arguments);
});

// HANDLE A UDP DGRAM MESSAGE
dgsocket.on('message', (msg, rinfo) => {
    console.log("Received UDP message: length ", msg.toString(), " with info = ", rinfo);

    // function that returns a message to the calling UDP socket.
    function sendReply(message) {
        // console.log("Sending UDP Reply: " + message);
        const fmsg = message + "\n";
        dgsocket.send(fmsg, 0, fmsg.length, rinfo.port, rinfo.address);
    }

// PROCESS THE DGRAM REQUEST!
    try {
        let filter_query = DEFAULT_QUERY;
        let force = false;
        let limit = 0;
        let source = null;
        let start = 0;
        if (msg.toString() && msg.toString().length > 1) {
            const msgObj = JSON.parse(msg);
            if (msgObj?.query) {
                filter_query = msgObj.query;
            }
            if (msgObj?.force) {
                force = (msgObj.force === "true");
            }
            if (msgObj?.limit) {
                limit = msgObj.limit;
            }
            if (msgObj?.source) {
                source = msgObj.source;
            }
            if (msgObj?.start) {
                start = msgObj.start;
            }
        }

        // REFACTOR: fuller request parameter passing
        processRequest({
            query: filter_query,
            force: force,
            limit: limit,
            source: source,
            start: start,
            orderNo: Date.now().toString(36)
        })
            .then((job) => {
                if (DEBUG) console.log("Request QUEUED with job data= ", job.data);
                const basemessage = {
                    data: job.data,
                    id: job.id
                };

                const processed = JSON.stringify({...basemessage, status: "queued"});

                // Handle events from the Job implemented in queue/jobCreationQueue.js
                job.on("succeeded", (job, data) => {
                    const message = JSON.stringify({
                        ...basemessage,
                        status: "succeeded",
                        data: data,
                        outcome: "how do we get this?"
                    }) + "\n";
                    sendReply(message);
                });

                job.on("failed", () => {
                    const message = JSON.stringify({...basemessage, status: "failed"});
                    sendReply(message);
                });

                job.on("retrying", () => {
                    const message = JSON.stringify({...basemessage, status: "retrying"});
                    sendReply(message);
                });

                job.on("progress", (progress) => {
                    // console.log("received progress: ", progress);

                    // INSERT LOGIC TO GIVE REASONABLE UPDATES:
                    // EVERY 25? in count
                    // AND WHEN done?

                    // console.log("PROGRESS: ", progress);

                    const count = progress.count.count;
                    const total = progress.count.number;
                    const writes = progress.count.classCount['write'] || 0;
                    const skips = progress.count.classCount['skip'] || 0;
                    // console.log("count: ", count,  " rem: ", count % 20);

                    if (count%20 === 0 || count >= total) {
                        const message = JSON.stringify({...basemessage, status: "progress", progress: progress});
                        sendReply(message);
                        console.log("REPORTED PROGRESS: ", progress);
                    }
                });

                sendReply(processed);
            })
            .catch((what) => {
                console.log("Request FAILED: ", what);

                // const error = JSON.stringify({
                //     status: "error",
                //     error: what
                // }) + "\n"
                dgsocket.send(what.toString(), 0, what.toString().length, rinfo.port, rinfo.address);
            });
    } catch (requestError) {
        console.log("Request Error: ", requestError);
        const errorMsg = JSON.stringify({
            status: "error",
            message: requestError.toString()
        });
        dgsocket.send(errorMsg, 0, errorMsg.length, rinfo.port, rinfo.address);
    }
});

// override expressjs limits
app.use(express.urlencoded({limit: "50mb", extended: true, parameterLimit: 50000}));
app.use(express.json({limit: "50mb"}));

// ...
// Add these lines before the Inits.

app.post('/post', (req, res) => {
    console.log("post received");


    // REFACTOR: fuller request parameter passing
    let order = {
        query: req.body.query,
        force: req.body.force,
        limit: (req.body.limit)?req.body.limit:0,
        source: req.body.source,
        orderNo: Date.now().toString(36)
    }

    if (order.query) {
        processRequest(order)
            .then(() => res.json({done: true, message: "Your order will be ready in a while"}))
            .catch(() => res.json({done: false, message: "Your order could not be placed"}));
    } else {
        res.status(422);
        res.send("{ \"error\":\"Problem with query\" }");
    }
});

app.get('/get', (req, res) => {
    let request = {
        query: req.params.query,
        orderNo: Date.now().toString(36)
    }

    if (request.query) {
        placeorder(request);
    } else {
        res.status(422);
        res.send("Turkey Lurkey");
    }

});


// Init Arena
const arena = Arena({
    // All queue libraries used must be explicitly imported and included.
    Bee,
    queues: [
        JOBCREATOR, INDEXER
    ],
}, {
    port: ARENA_PORT
});

app.use("/", arena);

// Create and start the server
const server = http.createServer(app);

if (UDP_PORT) {
    dgsocket.bind(UDP_PORT, () => {
        console.log(`UDP listener running at localhost:${UDP_PORT}`);
        // console.log("env: ", process.env);
    });
} else {
    console.log("No environment variable HTTP_PORT specified.  Not listening....");
}


if (HTTP_PORT) {
    server.listen(HTTP_PORT, () => {
        console.log(`HTTP Running at http://localhost:${HTTP_PORT}`);
    });
} else {
    console.log("No environment variable HTTP_PORT specified.  Not listening....");
}

module.exports = app;