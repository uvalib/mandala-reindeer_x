
const ts = module.exports.timestamp = module.exports.ts = () => new Date().toLocaleString();
module.exports.log = (...msgs) => {
    const stackString = (new Error()).stack;
    const ss = [...stackString.matchAll(/\((.*)\)+/g)]
    const location = (ss && ss[1] && ss[1][1])?" (" + ss[1][1] + ")":"";
    console.log(ts(), ...msgs, location );
}
