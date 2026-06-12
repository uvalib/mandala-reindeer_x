// process.env.PORT=5001;
process.env.ARENA_PORT=4568;
const app = require('../../server') // Link to your server file
const supertest = require('supertest')
const request = supertest(app);

describe('E2E TEST', () => {
    test('works', async () => {
        const res = await request.get('/');
        // await console.log("res = ", res);

        const post = await request.post("/post", () => {
            return "{ id: testy }";
        });
        // console.log(post);

    });
})