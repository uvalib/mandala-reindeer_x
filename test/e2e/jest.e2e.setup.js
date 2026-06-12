// HERE IS WHERE EVERYTHING NEEDED FOR INTEGRATIONS TESTS SHOULD BE SET UP
// e.g.  Redis
// const axios = require('axios');
// // const server = require( '../../server' );
// const port = 5000;
// const url = 'http://localhost:' + port + '/';
//
// const statusCheck = async () => {
//     try {
//         const res = await axios
//             .get(url);
//         return res.status === 200;
//     } catch (err) {
//         return false;
//     }
// };

module.exports = async () => {

    console.log("E2E (Integration) setup start...");
    // server(port);
    // console.log("...waiting for server to be up...");
    //
    // let isUp = false;
    // let retries = 5;
    // while (isUp === false  && retries > 0) {
    //     // console.log("checking " + url  + "\r") ;
    //     await new Promise((r) => setTimeout(r, 1000));
    //     isUp = await statusCheck();
    //     retries--;
    // }
    //
    // if (!isUp) {
    //    throw new Error("Server never came up! url = " + url);
    // }
    console.log("E2E (Integration) setup done!");
}
