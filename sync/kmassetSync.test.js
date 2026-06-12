import {createAssetEntry, createAssetDoc, getKmapEntries} from './kmassetSync';
import * as solr from 'solr-client';
import 'jest';
import 'mockshot';

const DEBUG=false;

const KMTERMS_DEV_UNAUTH = {
    'host': 'mandala-solr-proxy-dev.internal.lib.virginia.edu',
    'port': 443,
    'path': '/solr',
    'secure': true,
    'core': 'kmterms'
};

const test_kmapids = [
    ["lhasa", {
        uid: "places-637",
        name: ["Lasa", "Lhasa", "lha sa", "Rasa", "Shixiaqu", "ra sa" ],
        tree: "places"
    }],
    ["thod rgal/", {
        uid: "terms-85193",
        name: ["tögel", "thod rgal/", "direct transcendence"],
        tree: "terms"
    }],

    ["Language", {
        uid: "subjects-184",
        name: ["Language"],
        tree: "subjects"
    }],
    ["Language Context", {
        uid: "subjects-185",
        name: ["Language Context"],
        tree: "subjects"
    }],
    ["Literary Period", {
        uid: "subjects-187",
        name: ["Literary Period"],
        tree: "subjects"
    }],
    ["Register", {
        uid: "subjects-190",
        name: ["Register"],
        tree: "subjects"
    }],
    ["Topics", {
        uid: "subjects-272",
        name: ["Topics"],
        tree: "subjects"
    }],
    ["Grammars", {
        uid: "subjects-5812",
        name: ["Grammars"],
        tree: "subjects"
    }],
    ["Phoneme", {
        uid: "subjects-9310",
        name: ["Phoneme"],
        tree: "subjects"
    }],
    ["Tibetan Gramatical Function", {
        uid: "subjects-286",
        name: ["Tibetan Grammatical Function"],
        tree: "subjects"
    }],
    ["Geographic Features", {
        uid: "subjects-20",
        name: ["Geographical Features"],
        tree: "subjects"
    }],
];

// extract list of uid's
const uid_list = test_kmapids.map((x) => x[1].uid);

// remap by uid
const reducer = (accumulator, entry) => {
    const [name, data] = entry;
    const uid = data.uid;
    accumulator[uid] = data;
    return accumulator;
};
let test_kmapids_by_uid = test_kmapids.reduce(reducer, {});

// if (DEBUG) console.log(" test_kmapids_by_uid = ", test_kmapids_by_uid);

const list_query = "uid:" + JSON.stringify(uid_list)
    .replace(/\"/gi, "")
    .replace(/\[/g, "(")
    .replace(/\]/g, ")")
    .replace(/,/g, " ");

let read_client = solr.createClient(KMTERMS_DEV_UNAUTH);

let single_assets = {};

async function getSingleAsset(read_client,uid) {
    console.log("Getting Single Asset: uid="+uid);

    if (!read_client) {
        read_client = solr.createClient(KMTERMS_DEV_UNAUTH);
        await read_client.createQuery();
    }
    if (!single_assets[uid]) {
        const results = await getKmapEntries(read_client, "uid:" + uid, 10, 0);
        if (results?.length) {
            single_assets[uid] = results[0];
        }
    }
    return single_assets[uid];
}

beforeAll(
     async() => {

        if (DEBUG) console.log("beforeAll: readclient");
        await read_client.createQuery();
        if (DEBUG) console.log("beforeAll: readclient init done");

        const uid = test_kmapids[0][1].uid;
        if (DEBUG) console.log("beforeAll: retrieving uid = ", uid);
        single_assets[uid] = getSingleAsset(read_client,uid);
        if (DEBUG) console.log("beforeAll: retrieved uid = ", uid);
    }
);

describe('getKmapEntries retrieves a single kmaps entry', () => {
    test.each(test_kmapids)('returns expected data: %p', async (test_name, test_params) => {
        const {uid} = test_params;
        const ret = await getSingleAsset(read_client,uid);
        expect(ret).toHaveProperty("uid", uid);
        expect(ret).toMatchObject(test_params); // toMatchObject === all the given fields match
        expect(ret).toMatchSnapshot();
        // if (DEBUG) console.log("getKmapEntries: we have this: ", ret[0]);
    });
})

describe('getKmapEntries supports multiple kmaps entries', () => {

    let ret, ret_map

    beforeEach(async () => {
            ret = await getKmapEntries(read_client, list_query, 100, 0);
            ret_map = ret.reduce((collect, entry) => {
                collect[entry.uid] = entry;
                return collect;
            }, {});
        }
    )

    test("getKmapEntries returns a list of correct length", async () => {
        expect(ret).toHaveLength(uid_list.length);
    });

    test.each(uid_list)("getKmapEntries returns correct values: %p", async (x) => {
        expect(ret_map[x]).toMatchObject(test_kmapids_by_uid[x]);
    });

});

describe('createAssetDoc creates an Asset Entry from a Kmap Entry', () => {

    test('assets should have cascading_position_i', async() => {
        const uid = test_kmapids[1][1].uid;

        console.log("UID = ", uid);

        const asset = await getSingleAsset(read_client,uid);
        const value = asset.cascading_position_i;

        console.log("asset = ", asset);
        expect(value).toBeDefined();
        expect(typeof value).toBe("number");
    });

    test('test single assetEntry', async () => {
        const config = {kmaps_read_client: read_client};
        const kmapEntry = await getKmapEntries(read_client, "uid:places-637", 1, 0);
        // console.log("KMAP ENTRY:", kmapEntry);

        const asset_doc = await createAssetDoc(config, kmapEntry[0]);
        if (DEBUG) console.log("Asset Entry = ", kmapEntry);
        // console.error("ASSET DOC:", asset_doc);
        expect(JSON.stringify(asset_doc, undefined, 3)).toMatchSnapshot();
    });
});







