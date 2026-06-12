const async = require("async");
const _ = require("lodash");
const util = require("util");
const crypto = require("crypto");
const {processSubjectMapIdFacet} = require("./processSubjectMapIdFacet");
const {processRelateds} = require("./processRelateds");
const {processNames} = require("./processNames");

const {LocalStorage} = require("node-localstorage");
const localStorage = exports.localStorage = new LocalStorage('./scratch');

const striptags = require('striptags');
const {writeFile} = require("fs");
const fs = require("fs");
const DEBUG = false;
const SCHEMA_VERSION = exports.SCHEMA_VERSION = 41;
const FORCE_OVERWRITE = (process.env.FORCE_OVERWRITE === "true");

const ENABLE_LOG_DOC = (process.env.ENABLE_LOG_DOC === "true");
const FORCE_LOG_DOC_OVERWRITE = (process.env.FORCE_LOG_DOC_OVERWRITE === "true");

const LOCAL_DIR_PATH = process.env.LOCAL_DIR_PATH || "./output";
const NO_INDEX_WRITE = (process.env.NO_INDEX_WRITE === "true");
const getSolrSchemaChecksum = function (write_client, callback) {
    if (DEBUG) console.error("write_client = ", write_client);
    write_client.get('schema', (err, schema) => {
        if (DEBUG) console.log("getSolrSchemaChecksum(): returned schema: ", JSON.stringify(schema.schema, undefined, 3));
        let hash = "";
        if (!err) {
            hash = crypto.createHash('md5').update(JSON.stringify(schema.schema)).digest("hex");
            if (DEBUG) console.log("getSolrSchemaChecksum(): ", hash);
        } else {
            if (DEBUG) console.log("error while getting schema: ", err);
        }
        callback(err, hash);
    });
}

const getKmapEntriesCB = function (read_client, query, rows, start, callback) {
    // const DEBUG = false;
    if (DEBUG) console.log("getKmapEntries(): ARGUMENTS: " + JSON.stringify({
        query: query,
        rows: rows,
        start: start,
    }));

    // console.error ("read client: ", read_client);

    if (!read_client) {
        throw new Error("read_client is undefined!");
    }

    let main_query = read_client.createQuery()
        .q(query)
        .matchFilter("block_type", "parent")
        .rows(rows)
        .start(start);

    if (DEBUG) {
        console.log("QUERY:")
        console.dir(main_query, true);
    }
    let cacheKMapDocs = function (resp, cache_cb) {
        if (DEBUG) console.error("cacheKMapDocs: " + resp.response.docs.length);
        // console.dir(resp, true);
        let docs = resp.response.docs;
        // Let's cache a map of the uid's and headers
        for (var i = 0; i < docs.length; i++) {
            var doc = docs[i];
            // console.log("   -> " + doc.uid);
            if (!localStorage.getItem(doc.uid)) {
                if (DEBUG) console.log("Caching: " + doc.uid + " = " + doc.header);
                localStorage.setItem(doc.uid, doc.header);
            }
        }
        cache_cb(null, docs);
    };
    var readKMapEntry = function (query, read_cb) {
        if (DEBUG) console.log("readKmapEntry: query = ", JSON.stringify(query));
        read_client.search(query, function (err, resp) {
            if (err) {
                console.error("ERROR reading: " + err);
                console.error(" attempted query: " + JSON.stringify(main_query));
            } else {
                if (DEBUG) {
                    console.log("####### Response received");
                    console.log("numFound = " + resp.response.numFound);
                    console.log("start = " + resp.response.start);
                    // console.dir(resp.response.docs.length,true);
                }
            }
            read_cb(err, resp);
        });
    };

    // note this is per single doc
    var addRelatedPlaces = function (doc, related_cb) {
        let uid = doc.uid;
        let domain = "";
        let id = 0;

        // let DEBUG = false;
        [domain, id] = doc.uid.split('-');
        if (DEBUG) {
            console.error("addRelatedPlaces  uid    = " + uid + "  domain = " + domain + "  id     = " + id);
        }

        if (domain !== "subjects") {
            // return unaltered doc
            related_cb(null, doc);
        } else {

            if (DEBUG) console.log("########## Handling related places for subject: " + uid);

            let collector = function (document) {
                const ROWS = 300;
                let next_start = 0;
                let count = 0;
                let more = false;

                let inner = function () {
                };  // just a container function;


                let next = function (next_callback) {
                    let related_query = read_client.createQuery().q("feature_type_ids:" + id).rows(ROWS).start(next_start).fl("uid");
                    read_client.search(related_query, function (err, rel_resp) {
                        if (err) {
                            console.error("ERROR reading: " + err);
                            console.error(" attempted query: " + JSON.stringify(related_query));
                            next_callback(err, null);
                            return;
                        } else {
                            if (DEBUG) {
                                console.log("## Query:  " + JSON.stringify(related_query));
                                console.log("####### Response received");
                                console.log("numFound = " + rel_resp.response.numFound);
                                console.log("start = " + rel_resp.response.start);
                                // console.dir(rel_resp, true);
                            }
                        }

                        let numFound = rel_resp.response.numFound;
                        let start = rel_resp.response.start;
                        let rowsReturned = rel_resp.response.docs.length;
                        more = (start + rowsReturned < numFound);
                        next_start = start + rowsReturned + 1;

                        async.map(rel_resp.response.docs,
                            function (item, map_cb) {
                                // console.log(" related for " + uid + ": " + item.uid);
                                map_cb(null, item.uid);
                            },
                            function (err, relateds) {
                                if (relateds && relateds.length) {
                                    if (!document.kmapid) {
                                        document.kmapid = [];
                                    }
                                    document.kmapid = document.kmapid.concat(relateds);
                                    // console.log("relateds for " + document.uid + " =====> " + relateds.length + " total:" + document.kmapid.length + " " + ((more) ? "..." : ""));
                                    // console.log("kmapid =======> " + JSON.stringify(doc.kmapid));
                                }
                                next_callback(null, document);
                            });
                    });
                }

                var next_retry = async.retryable(
                    {
                        times: 5,
                        interval: function (attempts) {
                            var pause = 2000 * Math.pow(2, attempts) + 200;
                            console.log("pause on attempt " + attempts + ":" + pause);
                            return pause;
                        },
                        errorFilter: function (err) {
                            console.error("A: RETRY ON ERROR: " + err.code + " " + err.message);
                            if (err.code !== "ENOTFOUND") {
                                console.error("Unknown error: ", JSON.stringify(err));
                            }
                            return true;
                        }
                    },
                    function (cb) {
                        next(cb);
                    }
                );

                inner.next = next_retry;
                inner.done = function (document, done_cb) {
                    if (DEBUG) console.error(" more? " + more + " done? " + !more);
                    done_cb(null, !more);
                }
                return inner;
            }(doc);

            async.doUntil(collector.next, collector.done, function (err, resultDoc) {
                related_cb(null, resultDoc);
            });
        }
    };

    async.waterfall(
        [
            async.apply(readKMapEntry, main_query),
            cacheKMapDocs,
            (docs, addrelated_cb) => {
                async.map(docs, addRelatedPlaces,
                    (err, processed_docs) => {
                        // console.error("addRelatedPlaces returned: " + processed_docs.length );
                        addrelated_cb(null, processed_docs);
                    })
            }
        ],
        function (err, results) {
            if (DEBUG) console.error("END OF WATERFALL!");
            if (err) {
                console.error(err);
            }
            // console.log("result list: " + results.length);
            callback(err, results);
        }
    );
};

async function processFeatureTypes(config, domain, feature_types, feature_type_ids) {
    let idfacets_feature_types = [];
    if (domain === "places" && feature_types && feature_type_ids) {
        cacheKmap(feature_types, feature_type_ids, "subjects");
        for (let i = 0; i < feature_type_ids.length; i++) {
            const f = await lookupKmapIds(config, ["subjects-" + feature_type_ids[i]]);
            idfacets_feature_types.push(f[0]);
        }
    }
    return idfacets_feature_types;
}

const createAssetDoc = exports.createAssetDoc = async function (config, kmapEntry) {
    const type = kmapEntry.tree;
    const id = kmapEntry.uid.split("\-")[1];
    const uid = kmapEntry.uid;
    const header = kmapEntry.header;
    const domain = kmapEntry.tree;
    const feature_types = kmapEntry.feature_types;
    const feature_type_ids = kmapEntry.feature_type_ids;
    const idfacets_subjects = [];
    const idfacets_places = [];
    const idfacets_terms = [];
    let kmapid = [];

    const idfacets_feature_types = await processFeatureTypes(config, domain, feature_types, feature_type_ids);
    const [names, name_fields] = processNames(Object.entries(kmapEntry));

    if (DEBUG) {
        console.log(" NAMES ", names);
        console.log(" NAME FIELDS ", name_fields);
    }

    const text = _.flatten([names, kmapEntry.text]);
    if (DEBUG) console.log("text = ", JSON.stringify(text));

    // RELATEDS
    let relateds = processRelateds(kmapEntry);
    if (DEBUG) console.log("relateds = ", relateds.length);

    let stricts = [kmapEntry.uid];
    if (relateds) {
        stricts = _.concat(stricts, relateds);
    }
    const kmapList = await lookupKmapIds(config, relateds);

    // ANCESTORS
    if (kmapEntry.ancestor_uids_generic) {
        kmapid = kmapEntry.ancestor_uids_generic;
    } else if (kmapEntry['ancestor_uids_tib.alpha']) {
        kmapid = kmapEntry['ancestor_uids_tib.alpha'];
    }
    let ancestorsTxt = kmapEntry.ancestors;
    let ancestorIdsIs = kmapEntry.ancestor_ids_generic;

    //
    if (kmapEntry['ancestors_tib.alpha']) {
        ancestorsTxt = kmapEntry['ancestors_tib.alpha'];
    }

    //
    if (kmapEntry['ancestor_ids_tib.alpha']) {
        ancestorIdsIs = kmapEntry['ancestor_ids_tib.alpha'];
    }

    if (kmapEntry['ancestors_tib.alpha'] && !kmapEntry.ancestors) {
        kmapEntry.ancestors = kmapEntry['ancestors_tib.alpha'];
    }

    if (kmapEntry['ancestor_ids_tib.alpha'] && !kmapEntry.ancestor_ids_generic) {
        kmapEntry.ancestor_ids_generic = kmapEntry['ancestor_ids_tib.alpha'];
    }

    const uidlist = _.map(ancestorIdsIs, function (x) {
        return domain + "-" + x
    });
    const parent_uid = (uidlist.length > 1) ? uidlist[uidlist.length - 2] : "";

    if (kmapEntry.ancestors) {
        if (kmapEntry.ancestors.length !== kmapEntry.ancestor_ids_generic.length) {
            console.error("Counts don't match!  uid = " + uid);
            console.error(kmapEntry);
            // throw new Error ("Counts don't match for uid = " + uid);
            // next("count mistmatch uid = " + uid, kmapEntry);
        }
        cacheKmap(kmapEntry.ancestors, uidlist, domain);
    }

    let feature_type_uids = _.map(idfacets_feature_types, function (x) {
        return x.split('|')[1]
    });

    if (DEBUG) console.log("kmapid list construction from ", {
        "stricts": stricts,
        "relateds": relateds,
        "kmapid": kmapid,
        "uidlist": uidlist,
        "feature_types": feature_types
    })

    kmapid = _.uniq(_.sortBy(_.concat(stricts, relateds, kmapid, uidlist, feature_type_uids), function (x) {
        return x;
    }));

    let kmlist = await lookupKmapIds(config, kmapid);
    _.each(kmlist, function (x) {
        if (x.indexOf("|places") !== -1) {
            idfacets_places.push(x);
        } else if (x.indexOf("|subjects") !== -1) {
            idfacets_subjects.push(x);
        } else if (x.indexOf("|terms") != -1) {
            idfacets_terms.push(x);
        }
    });

    // extract perspectives
    const collected = _.reduce(kmapEntry, (result, value, key) => {
        const capt = key.match(/ancestor_id_([A-z_.]+)_path/);
        if (capt && capt[1]) {
            result[capt[1]] = 1;
        }
        return result;
    }, {});

    const perspectives_ss = Object.keys(collected);

    const name_tibt_sort = (kmapEntry.name_tibt && kmapEntry.name_tibt.length) ? kmapEntry.name_tibt[0] : null;
    const name_latin_sort = (kmapEntry.name_latin && kmapEntry.name_latin.length) ? kmapEntry.name_latin[0] : null;
    const title_latin_sort = (kmapEntry.header && kmapEntry.header.length) ? kmapEntry.header : null;

    // SPECIAL NUMERIC UID
    const uid_i = generateId(type + "-" + id);
    const kmapid_is = _.map(kmapid, generateId);

    // DOCUMENT TEMPLATE
    const doc = {
        ...name_fields,
        "schema_version_i": SCHEMA_VERSION,
        "asset_type": type,
        "service": config.service,
        "id": id,
        "uid": uid,
        "uid_i": uid_i,
        "url_html": config.baseurl + "/" + type + "/" + id + "/overview/nojs",
        "kmapid": kmapid,
        "kmapid_is": kmapid_is,
        "kmapid_strict": stricts,
        "text": text,
        "names_txt": names,
        "name_autocomplete": kmapEntry.name_autocomplete,
        "name_tibt": kmapEntry.name_tibt,
        "name_tibt_sort": name_tibt_sort,
        "name_latin": kmapEntry.name_latin,
        "name_latin_sort": name_latin_sort,
        "title": header,
        "title_latin_sort": title_latin_sort,
        "shapes_centroid_grptgeom": kmapEntry.shapes_centroid_grptgeom,
        "feature_types_ss": feature_types,
        "associated_subjects_ss": kmapEntry.associated_subjects,
        "ancestors_txt": ancestorsTxt,
        "ancestor_ids_is": ancestorIdsIs,
        "kmapid_subjects_idfacet": idfacets_subjects,
        "kmapid_places_idfacet": idfacets_places,
        "kmapid_terms_idfacet": idfacets_terms,
        "feature_types_idfacet": idfacets_feature_types,
        "related_uid_ss": relateds,
        "position_i": kmapEntry.position_i,
        "parent_uid": parent_uid,
        "perspectives_ss": perspectives_ss,
        "kmaps_timestamp": kmapEntry._timestamp_,
        "kmaps_version": kmapEntry._version_,
        "cascading_position_i": kmapEntry.cascading_position_i

    };

    // map projects_ss if there
    if (kmapEntry.projects_ss) doc['projects_ss'] = kmapEntry.projects_ss;

    // map the associated data if available
    if (kmapEntry.associated_subject_185_ss) doc["data_language_context_ss"] = kmapEntry.associated_subject_185_ss;
    if (kmapEntry.associates_subject_286_ss) doc["data_tibetan_grammatical_function_ss"] = kmapEntry.associates_subject_286_ss;
    if (kmapEntry.associated_subject_190_ss) doc["data_register_ss"] = kmapEntry.associated_subject_190_ss;
    if (kmapEntry.associated_subject_187_ss) doc["data_literary_period_ss"] = kmapEntry.associated_subject_187_ss;
    if (kmapEntry.associated_subject_5812_ss) doc["data_grammars_ss"] = kmapEntry.associated_subject_5812_ss;
    if (kmapEntry.associated_subject_272_ss) doc["data_tibet_and_himalayas_ss"] = kmapEntry.associated_subject_272_ss;
    if (kmapEntry.associated_subject_9310_ss) doc["data_phoneme_ss"] = kmapEntry.associated_subject_9310_ss;

    const subjectMapIdFacets = await processSubjectMapIdFacet(config, kmapEntry);
    if (subjectMapIdFacets.length) {
        doc['associated_subject_map_idfacet'] = subjectMapIdFacets;
    }

    // CLEAN CAPTIONS
    let caption = null;
    if (kmapEntry.caption_eng) {
        caption = cleanEntries(kmapEntry.caption_eng);
    }
    if (caption && caption.length) {
        doc.caption = caption;
    }

    // CLEAN TEXTS
    let newtext = null;
    if (kmapEntry.text) {
        newtext = cleanEntries(kmapEntry.text);
    }
    if (newtext && newtext.length) {
        doc.text = newtext;
    }

    // ILLUSTRATIONS

    const illustrations = kmapEntry.illustrations_images_thumb_ss;
    const illustrations_uids = kmapEntry.illustrations_images_uid_ss;

    doc.illustrations_images_thumb_ss = illustrations;
    doc.illustrations_images_uid_ss = illustrations_uids;

    // const illustrations = _.reduce(kmapEntry, (result, value, key) => {
    //     const capt = key.match(/illustrations_([A-z]+)_thumb_url/);
    //     if (capt && capt[1]) {
    //         result[capt[1]] = value;
    //     }
    //     return result;
    // }, {});
    //
    // if (DEBUG) console.error("ILLUSTRATIONS:", illustrations);
    //
    // if (Object.values(illustrations).length) {
    //     // put all the values into illustration_url_ss
    //     doc.illustrations_url_ss = _.flatten(Object.values(illustrations));
    //
    //     // iterate over the keys and sort them into the appropriate fields
    //     _.each(Object.keys(illustrations), function (x) {
    //         // console.log("KEY = ", x)
    //         if (x) {
    //             const ik = "illustration_url_" + x + "_ss";
    //             const kk = "illustration_" + x + "_url";
    //             // console.log("IKEY = " + ik);
    //             // console.log("KKEY = " + kk);
    //             doc[ik] = kmapEntry[kk];
    //         }
    //     });
    // }


    return doc;
}

const createAssetEntryCB = exports.createAssetEntryCB = function (kmapEntry, config, callback) {

    // console.error("createAssetEntryCB called with config = ", config,  "and kmapentry = " , kmapEntry);

    // let DEBUG = false;
    async.waterfall(
        [
            async.apply(getSolrSchemaChecksum, config.kmassets_write_client),
            async function (solr_schema_checksum, next) {
                // console.log("kmapEntry = ", kmapEntry.uid)
                const doc = await createAssetDoc(config, kmapEntry);

                // add checksum to document
                if (solr_schema_checksum) {
                    doc.solr_schema_checksum_s = solr_schema_checksum;
                }

                return doc;
                // next(null, doc);
            }
        ],
        function
            (err, doc) {

            if (DEBUG) console.log("DOC = ", doc);

            if (err) {
                console.error("ERROR reported:  " + err);
            }
            async.nextTick(function () {
                callback(err, doc)
            });
        }
    )

// let caption = (kmapEntry.caption_eng) ? kmapEntry.caption_eng : "Caption for " + kmapEntry.uid;
}

const checkAssetEntryCB = exports.checkAssetEntryCB = function (uid, config, callback) {
    const assets_read_client = config.kmassets_read_client;
    const query = assets_read_client.createQuery().df("uid").q(uid).rows(1).start(0);
    const get_retry = getRetryAssetQueryFunction(assets_read_client);
    get_retry(query, function (err, resp) {
        if (DEBUG) console.log("got: ", resp);
        callback(err, resp.response.docs);
        if (err) { console.error ("Error ", resp.response )};
    });
}

// Promisified
const getKmapEntries = exports.getKmapEntries = util.promisify(getKmapEntriesCB);
const createAssetEntry = exports.createAssetEntry = util.promisify(createAssetEntryCB);
const checkAssetEntry = exports.checkAssetEntry = util.promisify(checkAssetEntryCB);

// UTILITY FUNCTIONS

const cacheKmap = exports.cacheKmapIds = function cacheKmap(names, ids, domain) {

    if (typeof names !== "object") {
        // console.log("names is " + JSON.stringify(names));
        return;
    }

    if (typeof ids !== "object") {
        // console.log("ids is " + JSON.stringify(ids));
        return;
    }

    if (names.length !== ids.length) {
        console.log("lengths of the arrays do not match!");
        return;
    }

    for (let i = 0; i < names.length; i++) {
        const name = names[i];
        const id = ids[i];
        let uid = "";

        if (typeof id === "number") {
            uid = domain + "-" + id;
        } else {
            uid = id;
            const checktype = id.match(/(\w+)\-\d+/);
            if (!checktype || !checktype.length || checktype[1] !== domain) {
                throw new Error("CHECKTYPE: domain " + domain + " does not match id " + id + " with checktype = " + checktype[1]);
            }
        }

        const old = localStorage.getItem(uid);

        if (old) {
            if (old !== name) {
                // console.log ("#######################################################");
                let msg = "############## NAME MISMATCH: uid = " + uid + "\t old=" + old + " \t=> new=" + name;
                msg += "  Updating...";
                console.log(msg);
            }

            // update the cache
            localStorage.setItem(uid, name);
        } else {
            if (DEBUG) console.log("PUTTING: " + uid + "=>" + name);
            localStorage.setItem(uid, name);
        }

    }
}

const generateId = function (x) {
    const parts = x.split("-");
    const type = parts[0];
    let id = Number(parts[1]);
    id *= 100;

    if (type === "places") {
        id += 1;
    } else if (type === "subjects") {
        id += 2;
    } else if (type === "terms") {
        id += 3;
    } else {
        console.error("UNKNOWN kmap type: " + type + " from kmapid " + x);
    }
    return id;
}

var cleanEntries = function (entries) {
    let cleaned = [];
    for (let i = 0; i < entries.length; i++) {
        let stripped = striptags(entries[i]);
        stripped = stripped.replace('&nbsp;', '');
        if (stripped) {
            cleaned.push(stripped);
        }
    }
    return cleaned;
}

const lookupKmapIds = async function (config, kmapids) {

    // const DEBUG = false;
    // console.log("lookupKmapIds sees args = " + JSON.stringify(arguments));
    const kmapList = [];

    // console.log("KMAPIDS: " + JSON.stringify(kmapids));

    for (let i = 0; i < kmapids.length; i++) {
        const kid = kmapids[i];
        // console.log("lookupKmapIds looking up " + kid);
        let name = localStorage.getItem(kid);
        // console.log("lookupKmapIds looking up " + kid + " cache returned " + name);

        if (name === null) {
            // console.log("lookupKmapIds looking up " + kid + " via getKmapEntries");
            // console.log( "config = ", config);
            const entry = await getKmapEntries(config.kmaps_read_client, "uid:" + kid, 1, 0);
            if (DEBUG) {
                if (!entry.length) console.log(`getKmapEntries for ${kid} returned `, entry.length);
            }
            name = (entry.length) ? entry[0].header : kid;
        }
        const entry = name + "|" + kid;
        kmapList.push(entry);
    }

    // console.log("lookupKmapIds returning " + JSON.stringify(kmapList));
    return kmapList;
};

function getRetryAssetQueryFunction(asset_read_client) {
    return async.retryable({
            times: 5,
            interval: function (attempts) {
                const pause = 1000 * Math.pow(2, attempts);
                console.error("check RETRY: attempt ", attempts, ": retry waiting ", pause, " ms");
                return pause;
            },
            errorFilter: function (err) {
                console.error("check RETRY ON ERROR: " + err.code);
                if (err.code !== "ENOTFOUND") {
                    console.error("Unknown error: ", JSON.stringify(err));
                }
                return true;
            }
        },
        function (query, cb) {
            asset_read_client.get("select", query, function (err, resp) {
                if (err) {
                    err.query = JSON.stringify(query) ;
                }
                cb(err, resp);
            });
        }
    );
}

const writeAssetDocCB = exports.writeAssetDocCB =
    function (config, force_overwrite, new_doc, counter, callback) {

        if (!new_doc) {
            callback({error: "no document!"}, {skipped: true})
            return;
        }

        const target_write_client = config.kmassets_write_client;
        const target_read_client = config.kmassets_read_client;

        // TODO: make overwrite function overridable

        const timestampsMatch = function (tsa, tsb) {
            // console.log("timestampsMatch A = ",JSON.stringify(tsa));
            // console.log("timestampsMatch B = ",JSON.stringify(tsb));

            if (tsa === tsb) {
                return true;
            }

            /* Kludge:
               This allows for a little timestamp wiggle.
               Not sure why this happens, but we have seen
               timestamp discrepancies of several 100's of milliseconds)
             */
            const currentTimestamp = Date.parse(tsa);
            const storedTimestamp = Date.parse(tsb);
            // console.log(" Whacka:  kmapEntry._timestamp_         = ", tsa , "   long = ", currentTimestamp);
            // console.log(" Whacka:  existing_kmap.kmaps_timestamp = ", tsb, "   long = ", storedTimestamp );

            if (Math.abs(storedTimestamp - currentTimestamp) < 2000) {
                // console.log(kmapEntry.uid, "Whacka current kmap timestamp is same as stored.");
                return true;
            }

            return false;

        };


        const overwrite = function (newdoc, olddoc) {
            if (force_overwrite) {
                if (DEBUG) console.log("FORCE OVERWRITE ", newdoc.uid);
                return true;
            }

            if (!newdoc || Object.keys(newdoc).length === 0) {
                console.error("NEW DOC is empty...  Skipping...");
                return false;
            }

            if (!olddoc || Object.keys(olddoc).length === 0) {
                if (DEBUG) console.log("OVERWRITE LOGIC: !olddoc = ", !olddoc);
                if (DEBUG) console.log("OVERWRITE LOGIC: Object.keys(newdoc).length = ", Object.keys(newdoc).length);
                if (DEBUG) console.log("OVERWRITE LOGIC: returning true");
                return true;
            }

            if (!timestampsMatch(olddoc.kmaps_timestamp, newdoc.kmaps_timestamp)) {
                if (DEBUG) console.log("OVERWRITE LOGIC: olddoc.kmaps_timestamp = ", olddoc.kmaps_timestamp);
                if (DEBUG) console.log("OVERWRITE LOGIC: newdoc.kmaps_timestamp = ", newdoc.kmaps_timestamp);
                if (DEBUG) console.log("OVERWRITE LOGIC: kmaps_timestamps don't match. ", newdoc.uid)
                return true;
            }

            if (newdoc.schema_version_i !== olddoc.schema_version_i) {
                if (DEBUG) console.log("OVERWRITE LOGIC: newdoc.schema_version_i = ", newdoc.schema_version_i);
                if (DEBUG) console.log("OVERWRITE LOGIC: olddoc.schema_version_i = ", olddoc.schema_version_i);
                if (DEBUG) console.log("OVERWRITE LOGIC: schema_version_i's don't match. ", newdoc.uid);
                return true;
            }


            if (DEBUG) console.log("olddoc = ", JSON.stringify(olddoc));
            if (newdoc.solr_schema_checksum_s !== olddoc.solr_schema_checksum_s) {
                if (DEBUG) console.log("OVERWRITE LOGIC: solr_schema_checksum_s doesn't match!");
                return true;
            }

            // fall through default to false
            if (DEBUG) console.log("OVERWRITE LOGIC: fallthrough:  default to false");
            return false;
        };

        //  TODO: ys2n: FIX LOGIC HERE TO SEPARATE TARGET READ FROM TARGET WRITE client
        const query = target_read_client.createQuery().df("uid").q(new_doc.uid).rows(1).start(0);

        // wrap the "add" request in a retryable
        const add_retry = async.retryable(
            {
                times: 5,
                interval: function (attempts) {
                    const pause = 1000 * Math.pow(2, attempts);
                    console.log("pause on attempt " + attempts + ":" + pause);
                    return pause;
                },
                errorFilter: function (err) {
                    console.error("B: RETRY ON ERROR: " + err.code + " " + err.message);
                    console.error("ERROR OBJECT ", JSON.stringify(err));
                    console.error(query);
                    console.error(target_write_client);
                    if (err.code !== "ENOTFOUND") {
                        console.error("xxx===> Unknown error: ", err);
                    }
                    return true;
                }
            },
            async function (doc, cb) {

                if (DEBUG) console.log("=====DOC=====");
                if (DEBUG) console.log(doc);
                if (DEBUG) console.log("ENABLE_LOG_DOC = ", ENABLE_LOG_DOC);
                if (ENABLE_LOG_DOC) {
                    const filename = LOCAL_DIR_PATH + "/" + doc.uid + ".json";
                    if (DEBUG) console.log("FILE NAME = ", filename);

                    // create LOCAL_DIR_PATH if it doesn't exist already
                    await fs.exists(LOCAL_DIR_PATH, (exists) => {
                        if (DEBUG) console.log("fs exists callback: " + exists);
                        if (!exists) {
                            console.log("MAKING DIR ", LOCAL_DIR_PATH);
                            fs.mkdir(LOCAL_DIR_PATH, (err, ret) => {
                                if (err) {
                                    console.error("ERROR while trying to create dir: " + LOCAL_DIR_PATH + ": ", err)
                                } else {
                                    console.log("created directory: " + LOCAL_DIR_PATH);
                                }
                            });
                        }
                    });

                    fs.exists(LOCAL_DIR_PATH, (exists) => {
                        if (!exists || FORCE_LOG_DOC_OVERWRITE ) {
                            fs.writeFile(filename, JSON.stringify(doc, undefined, 3), {}, (err, msg) => {
                                if (err) {
                                    console.error("ERROR WRITING LOCAL FILE: " + filename);
                                    console.error(err);
                                } else {
                                    console.log("WROTE TO LOCAL FILE: " + filename);
                                }
                            });
                        } else {
                            console.log("... SKIPPING OVERWRITING EXISTING LOCAL FILE: " + filename);
                            // console.log("  ENABLE_LOG_DOC = " + ENABLE_LOG_DOC);
                            // console.log("  FORCE_LOG_DOC_OVERWRITE = " + FORCE_LOG_DOC_OVERWRITE);
                            // console.log("  FILE EXISTS = " + exists);
                        }
                    });
                }

                // if (NO_INDEX_WRITE) console.log("NO INDEX WRITE = " + NO_INDEX_WRITE);

                if (!NO_INDEX_WRITE) {
                    target_write_client.add(doc, cb);
                } else {
                    // console.log("NO_INDEX_WRITE: " + (NO_INDEX_WRITE) ? "true" : "false" + "... Skipping writing to the index");
                    cb(null,{
                        "no_index_write": "true",
                        "skipped": "true",
                        "responseHeader": {
                            "status": 0
                        }
                    });
                }
                ;
            },
        );

        // wrap the "check" query in a retryable
        const check_retry = getRetryAssetQueryFunction(target_read_client)

        // EXAMINE THIS ONE CAREFULLY
        // excecute the nested retryables
        check_retry(query, function (err, existing) {
            if (err) {
                console.error("error while trying to check entry: " + new_doc.uid + ": \n" + err);
                callback(err, null);
                return;
            }
            if (Object.keys(new_doc).length !== 0 &&
                (!existing.response.numFound || overwrite(new_doc, existing.response.docs[0]))) {

                if (DEBUG) console.log("WE ARE WRITING BECAUSE: ");
                //
                if (DEBUG) console.log("Object.keys(new_doc).length = ", Object.keys(new_doc).length);
                if (DEBUG) console.log("!existing.response.numFound = ", !existing.response.numFound);
                if (DEBUG) console.log("overwrite(new_doc, existing.response.docs[0]) = ", overwrite(new_doc, existing.response.docs[0]))

                const core = target_write_client.options.core;
                console.error(new Date().toLocaleTimeString() + " WRITING ASSET DOC: [" + core + "] " + counter.count() + "/" + counter.number() + " queued: " + counter.remain() + " " + new_doc.uid + ": " + JSON.stringify(new_doc.title));
                add_retry(new_doc, function (err, obj) {
                    if (err) {
                        console.error("ERROR: ", err);
                        // console.error("Problem writing: " + JSON.stringify(new_doc, undefined, 2));
                        console.error("Problem writing: " + new_doc.uid);

                        callback(err, obj);
                    } else {
                        if (obj?.responseHeader?.status !== 0) {
                            console.error('Solr response with non-zero status:', obj);
                        }
                        callback(null, obj);
                    }
                });
            } else {

                // TODO:  introduce reason....?
                if (DEBUG) console.log("skipping: " + new_doc.uid);
                callback(null, {skipped: true});
            }
        });
    };

const writeAssetDoc = exports.writeAssetDoc = util.promisify(writeAssetDocCB);
exports.lookupKmapIds = lookupKmapIds;
