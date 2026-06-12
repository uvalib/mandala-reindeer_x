const DEFAULT_CONCURRENCY = 10;
const DEBUG = false;
const {
    INDEXER,
    JOBCREATOR,
    REDIS,
    readConfig,
} = require('./queueConfigs');
const sync = require('../sync/kmassetSync');
const solr = require('solr-client');
const crypto = require('crypto');
const util = require("./util");
const _ = require('lodash');

const ENV_SETTING = process.env.ENV_SETTING;
const SOLR_DEV_USER = process.env.SOLR_DEV_USER;
const SOLR_DEV_PASS = process.env.SOLR_DEV_PASS;
const SOLR_PROD_USER = process.env.SOLR_PROD_USER;
const SOLR_PROD_PASS = process.env.SOLR_PROD_PASS;
const UDP_PORT = process.env.UDP_PORT;
const FORCE_OVERWRITE = process.env.FORCE_OVERWRITE;   // NOTE THIS IS A STRING TRUE/FALSE!
const NO_INDEX_WRITE = true; // always true for now!

const Queue = require('bee-queue');
const async = require("async");
const jobCreationQueue = new Queue(
    JOBCREATOR.name,
    {
        removeOnSuccess: INDEXER.removeOnSuccess,
        redis: JOBCREATOR.redis
    });
const indexerQueue = new Queue(
    INDEXER.name,
    {
        removeOnSuccess: INDEXER.removeOnSuccess,
        redis: INDEXER.redis
    });

let KMTERMS_UNAUTH = {};
let KMASSETS_AUTH = {};
let KMASSETS_UNAUTH = {};
let SOLR_PASS = "", SOLR_USER = "";
let SERVICE_NAME = "";
let BASE_URL = "";

SERVICE_NAME = "kmaps_dev";
BASE_URL = "https://dev.mandala.library.virginia.edu";

const CONFIG=readConfig();

CONFIG.kmassets_write_client.basicAuth(CONFIG.write_user, CONFIG.write_pass);

util.log("USING CONFIG = ", CONFIG);

// passed a list of jobs from the jobCreation queue
// set up x number of workers to process the jobs
const indexRequest = (query) => {
    health_active = true;
    const nameRoot = query.title||query.query||"untitled";
    const hash = crypto.createHash('md5').update(JSON.stringify(query)).digest("hex");
    const unique_id = nameRoot.toString() + "-" + query.start.toString() + "-" + hash + "-" + Date.now().toString(16);

    util.log(`INDEX REQUEST: new indexRequest (${unique_id}): `, query);

    return indexerQueue.createJob(query).setId(unique_id).save();
};

const processor = async (job) => {
    function getCounter(start_count) {
        var count = 0;
        var classCount = {};
        var full_count = start_count || 0;
        var remain = 0;
        var startTime = Date.now();
        const errorLog = [];

        var counter = {
            getStatus: function () {
                return {
                    count: count,
                    classCount: classCount,
                    number: full_count,
                    errorLog: errorLog
                }
            },
            time: function () {
                return Math.floor((Date.now() - startTime) / 1000);
            },
            done: function (cls, uid, err) {
                if (uid && err) {
                    const log = { uid: uid, error: err };
                    errorLog.push(JSON.stringify(log));
                }
                count++;
                if (typeof cls !== 'undefined') {
                    if (typeof classCount[cls] === 'undefined') {
                        classCount[cls] = 0;
                    }
                    classCount[cls]++;
                }
            },
            count: function () {
                return count;
            },
            number: function () {
                return full_count;
            },
            setCount: function (count) {
                full_count = count;
            },
            title: function () {
                return job.data.title;
            },
            remainingCallback: function (cb) {
                indexerQueue.checkHealth(function (err, count) {
                    if (err) {
                        util.error("error getting inactive count!")
                    } else {
                        remain = count.waiting;
                    }
                    cb(err, count);
                });
            },
            remain: function () {
                return remain;
            }
        };
        return counter;
    }

    util.log("indexerQueue: processor received job = ", job.data.title);

// given a solr query, and some batch parameters
// get a list of the kmapids that fit the query by querying the solr index
// generate jobs for the indexerQueue to process.

    let i = 0;
    // const timer = setInterval(() => {
    //     util.log("indexer thinking about ", job.data.start, " " + i++);
    // }, 500);

    const {query, rows, start, force} = job.data;
    const read_client = CONFIG.kmaps_read_client;
    const CONCURRENCY = 20;
    let counter = null;

    try {
        const list = await sync.getKmapEntries(read_client, query, rows, start);
        counter = getCounter(list.length);

        const outcomes = await async.mapLimit(list,
            CONCURRENCY,
            async (kmapEntry) => {
                let skip = false;
                try {
                    // TODO: CHECK TIMESTAMPS AND SCHEMA VERSION? TO SEE IF UPDATES ARE NECESSARY
                    if (DEBUG) util.log("trying to checkAssetEntry...", kmapEntry.uid, CONFIG);
                    const existing = await sync.checkAssetEntry(kmapEntry.uid, CONFIG);
                    // job.reportProgress({"existing": existing});
                    // util.log("existing = ", existing);
                    const existing_kmap = (existing && existing.length) ? existing[0] : {};
                    // util.log(kmapEntry);
                    const currentTimestamp = Date.parse(kmapEntry._timestamp_);
                    const storedTimestamp = Date.parse(existing_kmap.kmaps_timestamp);
                    // util.log(" Whacka:  kmapEntry._timestamp_         = ", kmapEntry._timestamp_ , "   long = ", currentTimestamp);
                    // util.log(" Whacka:  existing_kmap.kmaps_timestamp = ", existing_kmap.kmaps_timestamp, "   long = ", storedTimestamp );

                    if (Math.abs(storedTimestamp - currentTimestamp) < 2000) {
                        if (DEBUG) util.log(kmapEntry.uid, "current kmap timestamp is same as stored.");
                        skip = true;
                    }

                    if (sync.SCHEMA_VERSION !== existing_kmap.schema_version_i) {
                        if (DEBUG) util.log(kmapEntry.uid, ": schema version of existing entry does not match current schema version.  Not skipping");
                        skip = false;
                    }

                    if (!existing_kmap.solr_schema_checksum_s || existing_kmap.solr_schema_checksum_s !== kmapEntry.solr_schema_checksum_s) {
                        if (DEBUG) util.log(kmapEntry.uid, ": solr schema checksum of existing entry does not match current solr schema checksum.  Not skipping");
                        skip = false;
                    }

                    if (FORCE_OVERWRITE === "true" || force) {
                        skip = false;
                        if (DEBUG) util.log(kmapEntry.uid, "FORCE_OVERWRITE = ",FORCE_OVERWRITE, " force = ", force, " skip = ", skip);
                    }

                } catch (ex) {

                    if(DEBUG) util.log("ERRORING: ", ex);

                    counter.done("error", kmapEntry?.uid, ex);
                    job.reportProgress({
                        "job": job.id,
                        "uid": kmapEntry.uid,
                        "message": "error while checking timestamps",
                        "error": ex,
                        "count": counter?.getStatus()
                    });
                    return {uid: kmapEntry.uid, message: "error while checking " +
                            "timestamp ", error: true}
                }

                if (!skip) {

                    try {
                        const assetEntry = await sync.createAssetEntry(kmapEntry, CONFIG);
                        const write = await sync.writeAssetDoc(CONFIG, force, assetEntry, counter);

                        if (DEBUG) util.log("WRITE returned ", write, " for ", job.data.title);
                        if (!write?.skipped) {
                            counter.done("write");
                            job.reportProgress({
                                "job": job.id,
                                "uid": kmapEntry.uid,
                                "write": write,
                                "count": counter?.getStatus()
                            });
                            return {
                                job: job.id,
                                uid: kmapEntry.uid,
                                message: "written",
                                write: write,
                                count: counter?.getStatus()
                            };
                        } else {
                            counter.done("skip");
                            job.reportProgress({
                                "job": job.id,
                                "uid": kmapEntry.uid, "skipped": true, "reason": "writeAssetDoc logic",
                                "count": counter?.getStatus()
                            });
                            return {uid: kmapEntry.uid, message: "skipped", write: write, skipped: true};
                        }
                    } catch( e ) {
                        util.log("ERROR WHILE WRITING: ", e);
                        counter.done("error", kmapEntry.uid, e.message);
                        job.reportProgress({
                            "job": job.id,
                            "uid": kmapEntry.uid, "skipped": true, "reason": "Error while writing",
                            "error": e.message,
                            "count": counter?.getStatus()
                        });
                        throw new Error("Error while writing: " + e);
                    }
                } else {
                    if (DEBUG) util.log("skipping: " + job.id + " uid = " + job.uid);
                    counter.done("skip");
                    job.reportProgress({
                        "job": job.id,
                        "uid": kmapEntry.uid,
                        "skipped": true,
                        "reason": "timestamp",
                        "count": counter?.getStatus()
                    });
                    return {uid: kmapEntry.uid, message: "skipped", skipped: true};
                }
            });

        const writes = outcomes.filter(function (e) {
            return !e?.skipped
        });
        const skips = outcomes.filter((e) => e?.skipped);

        // util.log("ASSESSING OUTCOMES");
        // util.log(job.id, " SKIPPED:", skips.length, "/", outcomes.length);
        util.log("ASSESSING OUTCOMES: " , job.id," WRITES:", writes.length, "/", outcomes.length, " WRITTEN:", writes.map((x) => x?.uid).join(","));

        if (writes && writes.length && !NO_INDEX_WRITE) {
            util.log(`sending Soft commit for ${job.id} : ${job.data.title}`);
            CONFIG.kmassets_write_client.softCommit((err, ret) => {
                job.reportProgress({
                    "job": job.id,
                    "message": "softCommit callback",
                    "return": ret,
                    "error": err,
                    "count": counter?.getStatus()
                });
                util.log(`Soft commit of ${job.id} : ${job.data.title} returned: err=`, err, " ret=", ret);
            });
        } else {
            // console.log("no writes recorded.  no soft commit attempted.")
        }
        const report = {
            job_id: job.id,
            write_count: writes.length,
            skip_count: skips.length,
            soft_commit: (writes && writes.length),
            /* writes: writes, skips: skips */
        };

        // if (skips.length) {
        //     report.skip_list = JSON.stringify(skips.map(x => x.uid));
        // }

        if (writes.length) {
            report.write_list = writes.map(x => x.uid);
        }
        return JSON.stringify(report);

    } catch (err) {
        util.log("STACK: " , err.stack);
        util.log("ERROR processing  " + job.id, JSON.stringify(err.message));
        util.log(`Error from job ${job.id} : ${job.data.title}: `, JSON.stringify(err.message));
        counter?.done("error", job.id, err);
        job.reportProgress({
            "job": job.id,
            "message": `Error from job ${job.id} : ${job.data.title}`,
            "error": err.message,
            "count": counter?.getStatus()
        });
    } finally {
        // clearInterval(timer);
    }
};
indexerQueue.on('job progress', (jobId, progress) => {
    // if (!progress?.skipped) {
        if (DEBUG) util.log(`Job ${jobId} reported progress: `, progress);
    // }
});
indexerQueue.on('job failed', (jobId, err) => {
    util.log(`Job ${jobId} reported failure: `, err);
});
indexerQueue.on('job retrying', (jobId, err) => {
    util.log(`Job ${jobId} retrying on error
    : `, err);
});

indexerQueue.process(INDEXER.concurrency || DEFAULT_CONCURRENCY, processor);
indexerQueue.checkStalledJobs(5000, (err, num) => {
    if (num || err) {
        util.log("=== CHECK STALLED JOBS: returned: err = ", err, " num = ", num);
    }
});


// Print a report on the queue when it is active
// NB: approx. 15-second periods
const IDLE_CHECK_PERIOD = 240;
const ACTIVE_CHECK_PERIOD = 1;
let health_active = true;
let was_idle = false;
let idle_check_counter = 0;

const healthCheck = async () => {
    if (DEBUG) {
        util.log("HEALTH CHECK: health_active = ", health_active, " idle_check_counter = ", idle_check_counter);
    }
    const counts = await indexerQueue.checkHealth();
    const idle = !counts.active && !counts.waiting;
    if (health_active || idle_check_counter <= 0) {
        reportStatus();
        idle_check_counter = (health_active) ? 0 : IDLE_CHECK_PERIOD;
        if (was_idle && idle) {
            util.log("[ Indexer now idle ]");
            was_idle=false;
        }
    } else {
        // util.log("idling... ", idle_check_counter );
        idle_check_counter--;
    }

    if (!health_active && !idle) {
        util.log("[Indexer now active ]");
    }

    function reportStatus() {
        util.log("Indexer HEALTH: " + JSON.stringify(counts));
    }

    health_active = !idle;
};

healthCheck();
const periodCheck = setInterval(healthCheck, 15 * 1000);

// Exports
exports.indexRequest = indexRequest;
