const _ = require("lodash");
const kmassetSync = require("./kmassetSync");
const DEBUG = false;

const processSubjectMapIdFacet = exports.processSubjectMapIdFacet = async function (config, kmapEntry) {
    let collect = {};
    for (const [name, value] of Object.entries(kmapEntry)) {
        if (name.startsWith("associated_subject_")) {
            const match = name.match(/associated_subject_(\d+)_([a-z]+)/);
            if (match) {
                const [ undefined, subject_id, type ] = match;
                const subject = "subjects-" + match[1];
                let field = "";
                let final_value = "";
                if (type == "ls") {
                    field = "predicate_subject";
                    final_value = "subjects-" + value[0];
                } else if (type == "ss") {
                    field = "predicate_label";
                    final_value = value[0];
                }

                if (!collect[subject]) {
                    collect[subject] = {}
                }

                collect[subject][field] = final_value;
                collect[subject]["relation_subject"] = subject;
                let kmlist = await kmassetSync.lookupKmapIds(config, [subject]);
                collect[subject]["relation_label"] = kmlist[0].split('|')[0];
            }
        }
    }

    let output = [];
    if (!_.isEmpty(collect)) {
        Object.entries(collect).forEach(([name, entry]) => {
            const pre = entry["predicate_label"] + "=" + entry["predicate_subject"];
            const rel = entry["relation_label"] + "=" + entry["relation_subject"];

            let newEntry = rel + "|" + pre;
            output.push(newEntry);
        });
        if (output.length > 1) {
            if (DEBUG) console.log("MAPPING NEW FIELD ", kmapEntry.uid, " : ", output);
        }
    }
    return output;
}