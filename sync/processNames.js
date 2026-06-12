const _ = require("lodash");

const processNames =  exports.processNames = function (rentries) {

    // filter by name_* fields
    const name_entries = _.filter(rentries, function (x) {
        return x[0].startsWith("name_");
    });

    const name_fields = Object.fromEntries(name_entries);

    const name_values = _.map(name_entries, function (x) {
        return x[1];
    });


    // collect up and flatten the names
    const flat = _.flatten(name_values);
    const uniq = _.uniq(flat);
    const names = _.sortBy(uniq);
    return [ names, name_fields ];
}