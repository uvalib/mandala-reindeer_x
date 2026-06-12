const _  = require("lodash");

const processRelateds = exports.processRelateds = function (kmapEntry) {
    let relateds = [];
    if (kmapEntry.kmapid_strict) {
        relateds = kmapEntry.kmapid_strict;
    } else if (kmapEntry.kmapid) {
        relateds = kmapEntry.kmapid;
    }

    // add other relateds
    if (kmapEntry.associated_subject_ids) {
        relateds = _.uniq(_.concat(relateds, kmapEntry.associated_subject_ids)).map(function (x) {
            return "subjects-" + x
        });
    }
    return relateds;
}