/**
 * fileWatcher.js — folds the legacy `synch` (clsync) + `synchandler` (Perl/rclone)
 * pipeline into reindeer_x as a native Node.js subsystem (Spike 8, Part A).
 *
 * Watches one or more `solrdocs` output directories and uploads new/changed
 * Solr JSON documents to the kmassets ingest bucket using the AWS SDK — no
 * clsync, Perl, or rclone dependency at runtime.
 *
 * Behaviour is ported from docker/reindeer_x/files/usr/local/bin/synchandler.prod:
 *   - only non-empty `*.json` files are uploaded (the Perl `-s` test)
 *   - the per-site "app" segment is derived from the watched path so the S3
 *     key matches `s3://{bucket}/{prefix}/{app}/{file}`
 *   - `*.ids` files (asset deletions) are routed to a separate delete prefix,
 *     per the Spike 8 work plan.
 */

const fs = require("fs");
const path = require("path");
const chokidar = require("chokidar");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");

// Mirrors the synchandler.prod regex: pull the site host out of an Aegir/Drupal
// solrdocs path so uploads land under the correct per-site prefix.
const APP_PATH_RE = /(mandala-[\w-]+\.lib\.virginia\.edu)\/private\/files\/solrdocs/;

const DEFAULT_APP = "mandala.lib.virginia.edu";
const UPLOAD_EXTENSIONS = new Set([".json", ".ids"]);

function deriveApp(filePath) {
    const match = filePath.match(APP_PATH_RE);
    return match ? match[1] : DEFAULT_APP;
}

function s3KeyFor(filePath, { prefix, deletePrefix }) {
    const base = path.basename(filePath);
    const app = deriveApp(filePath);
    const isDelete = path.extname(filePath) === ".ids";
    return `${isDelete ? deletePrefix : prefix}/${app}/${base}`;
}

/**
 * Upload a single file to S3. Returns the written key, or null if the file was
 * skipped (empty file — matches the legacy non-empty `.json` filter).
 */
async function uploadFile(s3, bucket, filePath, config) {
    let stat;
    try {
        stat = fs.statSync(filePath);
    } catch (err) {
        // File vanished between the watcher event and the upload — nothing to do.
        if (err.code === "ENOENT") return null;
        throw err;
    }
    if (!stat.isFile() || stat.size === 0) return null;

    const key = s3KeyFor(filePath, config);
    await s3.send(new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: fs.createReadStream(filePath),
        ContentType: "application/json",
    }));
    return key;
}

function resolveConfig(overrides = {}) {
    const env = process.env;
    const watchDirs = (overrides.watchDirs
        || (env.WATCH_DIRS && env.WATCH_DIRS.split(",").map((d) => d.trim()).filter(Boolean))
        || [env.LOCAL_DIR_PATH || "/opt/output"]);

    return {
        enabled: overrides.enabled !== undefined
            ? overrides.enabled
            : env.ENABLE_FILE_WATCHER === "true",
        watchDirs,
        bucket: overrides.bucket || env.INGEST_BUCKET,
        prefix: overrides.prefix || env.INGEST_PREFIX || "kmassets-inbound/test",
        deletePrefix: overrides.deletePrefix || env.INGEST_DELETE_PREFIX || "kmassets-delete",
        region: overrides.region || env.AWS_REGION || "us-east-1",
        // chokidar polling — needed for some Docker bind-mount filesystems where
        // inotify events don't propagate (Spike 8 fail-criteria mitigation).
        usePolling: overrides.usePolling !== undefined
            ? overrides.usePolling
            : env.WATCHER_USE_POLLING === "true",
    };
}

/**
 * Start watching the configured directories. Returns the chokidar watcher
 * (so callers/tests can close it), or null when disabled or misconfigured.
 */
function startFileWatcher(overrides = {}) {
    const config = resolveConfig(overrides);

    if (!config.enabled) {
        console.log("[fileWatcher] disabled (set ENABLE_FILE_WATCHER=true to enable)");
        return null;
    }
    if (!config.bucket) {
        console.error("[fileWatcher] INGEST_BUCKET not set — file watcher will not start");
        return null;
    }

    const s3 = overrides.s3Client || new S3Client({ region: config.region });

    const watcher = chokidar.watch(config.watchDirs, {
        ignoreInitial: true,           // clsync only acts on changes, not the backlog
        usePolling: config.usePolling,
        awaitWriteFinish: {            // don't upload partially-written docs
            stabilityThreshold: 500,
            pollInterval: 100,
        },
    });

    const handle = (filePath) => {
        if (!UPLOAD_EXTENSIONS.has(path.extname(filePath))) return;
        uploadFile(s3, config.bucket, filePath, config)
            .then((key) => {
                if (key) console.log(`[fileWatcher] uploaded ${filePath} -> s3://${config.bucket}/${key}`);
            })
            .catch((err) => {
                console.error(`[fileWatcher] upload failed for ${filePath}: ${err.message}`);
            });
    };

    watcher
        .on("add", handle)
        .on("change", handle)
        .on("error", (err) => console.error(`[fileWatcher] watcher error: ${err.message}`))
        .on("ready", () => console.log(
            `[fileWatcher] watching [${config.watchDirs.join(", ")}] -> `
            + `s3://${config.bucket}/${config.prefix}`));

    return watcher;
}

module.exports = {
    startFileWatcher,
    uploadFile,
    s3KeyFor,
    deriveApp,
    resolveConfig,
};
