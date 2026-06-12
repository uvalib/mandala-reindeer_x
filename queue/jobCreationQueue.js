const DEBUG = false;
const nodeutil = require("util");
const util = require("./util");
const {INDEXER, JOBCREATOR, REDIS, readConfig } = require('./queueConfigs');
const NO_WRITE = false;
const {indexRequest} = require('./indexerQueue');
const DEFAULT_ROWS = 50;
// const solr = require('solr-client');
require('dotenv').config();
const async = require('async');
const _ = require('lodash');
const ENV_SETTING = process.env.ENV_SETTING || "dev";  // possible values:  "prod","stage","dev","predev"
const CONFIG = readConfig(ENV_SETTING);
const Queue = require('bee-queue');
const jobCreationQueue = new Queue(
    CONFIG.JOBCREATOR.name,
    {
        removeOnSuccess: CONFIG.INDEXER.removeOnSuccess,
        redis: CONFIG.JOBCREATOR.redis
    });
const indexingQueue = new Queue(
    CONFIG.INDEXER.name,
    {
        removeOnSuccess: CONFIG.INDEXER.removeOnSuccess,
        redis: CONFIG.INDEXER.redis
    });

//  NEED TO COLLECT STATS ON A JOB AND RETURN IT UPON "SUCCESS"/{

// util.log("I AM JOBCREATOR, HEAR ME ROAR");

const processRequest = (request) => {
    if (DEBUG) util.log("jobCreationQueue processRequest query=", request);
    return jobCreationQueue.createJob(request).setId(Date.now().toString()).save();
};

const processor = async (job) => {
    try {
        if (DEBUG) util.log("jobCreationQueue: processor called with ", job.data);
        const specs = await generateJobspecs(CONFIG, job.data);

        if (NO_WRITE) {
            util.log(`NO_WRITE is ${NO_WRITE}:  skipping writing...`);
            return;
        }

        return Promise.allSettled(await _.map(specs, (spec) =>
            new Promise(async (resolve, reject) => {
                try {
                    const subjob = await indexRequest(spec);

                    //  onsole.log("RETURN: " , subjob);
                    if (subjob?.status === "created") {
                        resolve({"result": "success", "index_job_id": subjob.id, "spec": spec});
                    } else {
                        reject({"result": "failure", "spec": spec, "return": subjob});
                    }

                    subjob.on('succeeded', (result) => {
                        util.log(`Received result for job ${subjob.id}: ${result}`);
                        resolve(result);
                    });

                    // subjob.on('retrying', (err) => {
                    //     util.log(`Retrying job ${subjob.id}: ${err}`);
                    // });
                    //
                    // subjob.on('failed', (err) => {
                    //     util.log(`Failed job ${subjob.id}: ${err}`);
                    //     reject(err);
                    // });
                    //
                    // subjob.on('progress', (progress) => {
                    //     // util.log(`Progress on job ${subjob.id}: ${JSON.stringify(progress)}`);
                    //     // util.log("Passing progress to parent job");
                    //     job.reportProgress(progress);
                    // });
                } catch (e) {
                    reject({
                        "result": "error", "error": e
                    });
                }
            }).then((result) => {
                if (DEBUG) util.log("promJobs: then result = ", result);
            }).catch((err) => {
                util.log("ERROR promJobs: err = ", err);
            }).finally(() => {
                if (DEBUG) util.log("promJob finally");
            })
        ))
    } catch (e) {
        util.log("generateJobspec failed: ", e);
        util.log("job data: ", job.data );
        return (e);
    }
};

function generateQuery(client, query, fq_list, start, rows) {
    // util.log("jobCreationQueue::generateQuery: arguments =", arguments);
    const nq = client.createQuery().q(query);
    fq_list.forEach(x => {
        const [field, value] = x.split(":");
        nq.matchFilter(field, value);
    });
    nq.start(start);
    nq.rows(rows);

    // for diagnostic purposes
    nq.set("echoParams=explicit");
    return nq;
}

const generateJobspecs = exports.generateJobspecs = nodeutil.promisify(function (config, jobdata, callback) {
    if (DEBUG) util.log("generateJobspecs: called with jobdata ", jobdata)
    const query = jobdata.query;
    const force = jobdata.force || false;
    const source = jobdata.source || "kmaps";
    const limit = jobdata.limit || 0;   // 0 === unlimited
    const orderNo = jobdata.orderNo || Date.now().toString(16);
    const base_start = jobdata.start || 0;
    const default_fq_list = [];
    if (source === "kmaps") {
        default_fq_list.push("block_type:parent");
    } else if (source === "kmassets") {
        default_fq_list.push("asset_type:(subjects places terms)");
    }
    let fq = [];
    if (jobdata.fq) {
        // wrap in array if its not already
        fq = (_.isArray(jobdata.fq) ? jobdata.fq : [jobdata.fq]);
    }
    const fq_list = [...default_fq_list, ...fq];

    if (DEBUG) {
        util.log("generateJobspecs: jobdata=", jobdata);
        util.log("generateJobspecs: ", {query: query, source: source, fq_list: fq_list});
    }

    let client = config.kmaps_read_client;
    if (source === "kmassets") {
        client = config.kmassets_read_client;
    }

    if (DEBUG) {
        util.log("CLIENT = " + JSON.stringify(client, undefined, 3));
    }

    var rows = config.rows || DEFAULT_ROWS;
    if (limit > 0 && limit < rows) {
        rows = limit;
    }
    if (rows === 0) {
        util.log("ROWS was zero!  forced to 1");
        rows = 1;
    }

    const q = generateQuery(client, query, fq_list, 0, 0);
    if (DEBUG) util.log("query build = ", q.build());
    client.search(q, function (err, results) {
        if (err) {
            callback(err);
        } else {

            if (DEBUG) util.log("initial solr query results = ", results);
            let num = results.response.numFound;
            if (limit && num > limit) {
                util.log("NOTE: limiting results to: ", limit);
                num = limit;
            }
            const chunks = Math.ceil(num / rows);
            const specs = [];
            if (DEBUG) util.log(" chunks = ", chunks);
            let target_query = query;
            if (source === 'kmassets') {
                // We're now doing this via kmassets so we need to
                // break query into uid list

                let i = 0;
                async.until(
                    (cb) => {
                        if (DEBUG) util.log("test i = ", i, (i * rows > num))
                        cb(null, (i * rows >= num));
                    },
                    (next) => {
                        // if (DEBUG) util.log("iterate");
                        const start = i * rows;
                        const nq = generateQuery(client, query, fq_list, start, rows);
                        nq.fl(["uid"]); // we only need the uid
                        if (DEBUG) util.log({start: start, rows: rows});
                        // if (DEBUG) util.log("prequery build", nq.build());
                        client.search(nq, (err, results) => {
                            // if (DEBUG) util.log("err = ", err, "docs = ", results.response.docs);
                            if (!err) {
                                const uidlist = results.response.docs.map(x => x.uid);
                                // util.log("uidlist = ", uidlist);
                                target_query = "uid:(" + uidlist.join(' ') + ")";
                                const job = {
                                    source: source,
                                    query: target_query,
                                    fq: fq_list,
                                    title: `${source}: q=${query} fq=${fq_list.length} rows=${rows} start=${start}`,
                                    start: 0,
                                    rows: rows
                                };
                                specs.push(job);
                                if (DEBUG) util.log("pushing ", job);
                            } else {
                                util.error("Error reported on query: ", {query: nq, error: err});
                            }
                            i++;
                            next(err);
                        });
                    },
                    (err) => {
                        callback(err, specs);
                    }
                );
            } else {
                // source === 'kmaps'
                for (let i = 0; i < chunks; i++) {
                    const start = i * rows;
                    if (DEBUG) util.log(" num = ", num, " rows = ", rows, " chunks = ", chunks, " start = ", start, " i = ", i);
                    const job = {
                        source: source,
                        query: target_query,
                        fq: fq_list,
                        title: `${source}: q=${query} fq=${fq_list.length} rows=${rows} start=${start}`,
                        start: start,
                        rows: rows,
                        force: force,
                    };
                    if (DEBUG) util.log("pushing ", job);
                    specs.push(job);
                }
                callback(null, specs);
            }
        }
    })
});


jobCreationQueue.on('job progress', (jobId, progress) => {
    // if (!progress.skipped) {
    //     util.log(`Job ${jobId} reported progress: `, progress);
    // }
});
jobCreationQueue.on('job failed', (jobId, err) => {
    util.log(`Job ${jobId} reported failure: `, err);
});
jobCreationQueue.on('job retrying', (jobId, err) => {
    util.log(`Job ${jobId} retrying on error: `, err);
});


const beequeueName = "JobCreationQueue";

// Queue Local Events
//   from https://github.com/bee-queue/bee-queue#queue-local-events
jobCreationQueue.on('job error', (err) => util.log(`LocalEvent: QUEUE ${beequeueName} A queue error happened: ${err.message}`));
jobCreationQueue.on('job succeeded', (job, result) => util.log(`LocalEvent: QUEUE ${beequeueName} succeeded: Job ${job.id} succeeded with result: ${JSON.stringify(result)}`));
// Unrelated bug: this event doesn't happen, even though the job is retried and the job gets the 'retrying' event
// the queue doesn't receive the retrying event
// See: https://github.com/bee-queue/bee-queue/issues/184
jobCreationQueue.on('job retrying', (job, err) => util.log(`LocalEvent: QUEUE ${beequeueName} retrying: Job ${job.id} failed with error '${err.message}' and retries ${job.options.retries}`));
jobCreationQueue.on('job failed', (job, err) => util.log(`LocalEvent: QUEUE ${beequeueName} failed: Job ${job.id} failed with error '${err.message}', and retries ${job.options.retries}, status ${job.status}`));
jobCreationQueue.on('job stalled', (jobId) => util.log(`LocalEvent: QUEUE ${beequeueName} stalled: Job ${jobId} stalled, will be retried`));


const IDLE_CHECK_PERIOD = 3;
// const ACTIVE_CHECK_PERIOD = 1;
let health_active = true;
let idle_check_counter = 0;
let was_idle = false;
const healthCheck = async () => {
    if (DEBUG) {
        util.log("HEALTH CHECK: health_active = ", health_active, " idle_check_counter = ", idle_check_counter);
    }
    const counts = await jobCreationQueue.checkHealth();
    const idle = !counts.active && !counts.waiting;
    if (health_active || idle_check_counter <= 0) {
        reportStatus();
        idle_check_counter = (health_active) ? 0 : IDLE_CHECK_PERIOD;
        if (was_idle && idle) {
            util.log("[ JobCreator now idle ]");
            was_idle = false;
        }
    } else {
        // util.log("idling... ", idle_check_counter );
        idle_check_counter--;
    }

    if (!health_active && !idle) {
        util.log(" [JobCreator now active ]");
    }

    function reportStatus() {
        util.log("JobCreator HEALTH: " + JSON.stringify(counts));
    }

    health_active = !idle;
};

    jobCreationQueue.checkStalledJobs(5000, (err, num) => {
        if (num || err) {
            util.log("=== jobCreationQueue: CHECK STALLED JOBS: returned: err = ", err, " num = ", num);
        }
    }).then(r => {
    });

healthCheck().then(r => {}).catch(e => {});
setInterval(healthCheck, 15 * 1000);

jobCreationQueue.process(8, processor);
module.exports.processRequest = processRequest;
