// For a detailed explanation regarding each configuration property, visit:
// https://jestjs.io/docs/en/configuration.html
const path = require('path');

module.exports = {

    name: "e2e",
    displayName: "E2E Integration Tests",
    globalSetup: path.join(__dirname, 'jest.e2e.setup.js'),
    // projects: [ '<rootDir>/test/*' ],

    testEnvironment: "node",
    testLocationInResults: true,
    testMatch: [
        "**/__tests__/**/*.[jt]s?(x)",
        "**/?(*.)+(spec|test).[tj]s?(x)"
    ]
};
