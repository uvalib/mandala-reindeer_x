const solr = require('solr-client');


const REDIS = module.exports.REDIS = {
    url: process.env.REDIS_URL,
    password: process.env.REDIS_PASS,
};

const JOBCREATOR = module.exports.JOBCREATOR = {
    name: 'jobcreator',
    hostId: 'Main',
    type: 'bee',
    prefix: 'bq',
    removeOnSuccess: false,
    redis: REDIS
};

const INDEXER = module.exports.INDEXER = {
    name: 'indexer',
    hostId: 'Main',
    type: 'bee',
    prefix: 'bq',
    removeOnSuccess: false,
    redis: REDIS
};

module.exports.readConfig = () => {
    const KMTERMS_UNAUTH = {
        'host': process.env.KMTERMS_UNAUTH_HOST,
        'port': process.env.KMTERMS_UNAUTH_PORT,
        'family': process.env.KMTERMS_UNAUTH_FAMILY || 4,
        'path': process.env.KMTERMS_UNAUTH_PATH || '/solr',
        'secure': process.env.KMTERMS_UNAUTH_SECURE === "true",
        'core': process.env.KMTERMS_UNAUTH_CORE || "kmterms",
        'solrVersion': process.env.KMTERMS_UNAUTH_SOLRVERSION,
    }

    const KMASSETS_AUTH = {
        'host': process.env.KMASSETS_AUTH_HOST,
        'port': process.env.KMASSETS_AUTH_PORT,
        'family': process.env.KMASSETS_AUTH_FAMILY || 4,
        'path': process.env.KMASSETS_AUTH_PATH || '/solr',
        'secure': process.env.KMASSETS_AUTH_SECURE === "true",
        'core': process.env.KMASSETS_AUTH_CORE || "kmassets",
        'solrVersion': process.env.KMASSETS_AUTH_SOLRVERSION,
        'autoCommit': process.env.AUTO_COMMIT === "true"
    }

    const KMASSETS_UNAUTH = {
        'host': process.env.KMASSETS_UNAUTH_HOST,
        'port': process.env.KMASSETS_UNAUTH_PORT,
        'family': process.env.KMASSETS_UNAUTH_FAMILY || 4,
        'path': process.env.KMASSETS_UNAUTH_PATH || '/solr',
        'secure': process.env.KMASSETS_UNAUTH_SECURE === "true",
        'core': process.env.KMASSETS_UNAUTH_CORE || "kmassets",
        'solrVersion': process.env.KMASSETS_UNAUTH_SOLRVERSION
    }

    const CONFIG = {
        "kmaps_read_client": solr.createClient(KMTERMS_UNAUTH),
        "kmassets_write_client": solr.createClient(KMASSETS_AUTH),
        "kmassets_read_client": solr.createClient(KMASSETS_UNAUTH),
        "service_name": process.env.SERVICE_NAME,
        "baseurl": process.env.MANDALA_BASEURL,
        "JOBCREATOR": JOBCREATOR,
        "INDEXER": INDEXER,
        "write_user": process.env.SOLR_WRITE_USER,
        "write_pass": process.env.SOLR_WRITE_PASS
    }
    return CONFIG;
}