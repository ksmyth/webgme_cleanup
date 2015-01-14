var argv = require('minimist')(process.argv.slice(2), {
'string': ['d', 'db', 'c', 'collection'],
'boolean': ['n', 'dry-run', 'squash'],
unknown: function (opt) { console.error("Unknown option " + opt); process.exit(1); }
});

var connstr = 'mongodb://127.0.0.1:27017/' + (argv.d || argv.db);
var collectionName = argv.c || argv.collection;

var squash = argv.squash;
if (!squash) {
    console.error("Preserving commit history is unimplemented. Pass the --squash switch to remove commit history.");
    process.exit(1);
}
var dryrun = argv.n || argv['dry-run'];

var mongodb = require('mongodb');
var Q = require('q');
var limitConcurrency = require('./limitConcurrency');

var findOne = function(query) {
    return Q.ninvoke(collection, 'findOne', query);
};
limitConcurrency(findOne, 10);

var all_hashes = {};
var commit_hashes = {};
var getAllReferencedObjects = function(hash) {
    all_hashes[hash] = 1;
    return findOne({_id: hash})
        .then(function (obj) {
            if (obj === null) {
                console.error("Warning: could not find object " + JSON.stringify(hash));
                return Q.all([]);
            }
            ['ovr', 'reg', '_meta', '_sets', '_nullptr', 'atr', '_id', '_constraints'].forEach(function (ignore) {
                delete obj[ignore];
            });
            var ret = [];
            for (key in obj) {
                if (typeof obj[key] !== 'string') {
                    throw new Error("non-string " + key);
                }
                ret.push(getAllReferencedObjects(obj[key]));
            }
            //console.log(JSON.stringify(obj, null, 4));
            return Q.all(ret);
        });
};

var db;
var collection;
var future = Q.nfcall(mongodb.MongoClient.connect, connstr)
    .then(function (db_) {
        db = db_;
        collection = db.collection(collectionName);

        return Q.ninvoke(collection.find({$and: [{_id: {$nin: ['*info*']}}, {_id: {"$regex": /^\*/}}]}), 'toArray');
    }).then(function (branches) {
        var ret = [];
        branches.forEach(function (branch) {
            if (branch.hash) {
                ret.push(Q.ninvoke(collection, 'findOne', {_id: branch.hash}));
            } else {
                ret.push(null);
            }
        });
        return Q.all([Q.all(ret), branches]);
    }).then(function (arg) {
        var commits = arg[0];
        var branches = arg[1];
        var ret = [];
        for (var i = 0; i < branches.length; i++) {
            if (commits[i]) {
                //console.dir(commits[i])
                commit_hashes[commits[i]._id] = 1;
                all_hashes[commits[i]._id] = 1;
                ret.push(commits[i].root)
                console.log("Branch '" + branches[i]._id.substr(1) + "': " + branches[i].hash);
                if (!squash) {
                    throw new Error("unimplemented"); // TODO: go through .parents and add
                }
            } else {
                console.error("Warning: branch '" + branches[i]._id.substr(1) + "' points to non-existant hash " + branches[i].hash);
            }
        }
        return ret;
    }).then(function (roots) {
        //console.log(roots)
        var ret = [];
        roots.forEach(function (root) {
            ret.push(getAllReferencedObjects(root));
        });
        return Q.all(ret);
    });
if (dryrun) {
    future = future.then(function () {
        console.log("Number of objects to keep: " + Object.getOwnPropertyNames(all_hashes).length);
        return Q.ninvoke(collection.find({$and: [{_id: {$regex: /^#/}},
                                                 {_id: {$nin: Object.getOwnPropertyNames(all_hashes)}}]}), 'count');
    }).then(function (count) {
        console.log("" + count + " object would be deleted");
    });
} else {
    future = future.then(function () {
        return Q.ninvoke(collection, 'remove', {$and: [{_id: {$regex: /^#/}},
                                                 {_id: {$nin: Object.getOwnPropertyNames(all_hashes)}}]});
    }).then(function (count, _) {
        console.log("" + count + " objects were deleted");
    });
    if (squash) {
        future = future.then(function() {
        console.dir(commit_hashes);
            return Q.ninvoke(collection, 'update', {_id: {$in: Object.getOwnPropertyNames(commit_hashes)}}, {$set: {parents: []}});
        });
    }
}

future.catch(function (error) {
        console.error(error.stack);
    }).finally(function (){
        db.close();
    })
    .done();
