// This test written in mocha+should.js
var should = require("./init.js");
var db, User;

describe('basic-querying', function() {



    before(function(done) {
        db = getSchema();

        User = db.define('User', {
            realm: {
                type: String,
                id: 1,
                keyType: "hash",
                limit: 100
            },
            role: {
                type: String,
                sort: true,
                limit: 100
            },
            id: {
                type: String,
                id: 2,
                keyType: "sort"
            },
            username: {
                type: String,
            },
            order: {
                type: Number,
                sharding: true,
                splitter: "10kb"
            }
        });
            
        db.adapter.emitter.on("created-user", function() {
            User.destroyAll(done);
        });
        done();
    });


    describe('find', function() {

        before(function(done) {
            done();
        });

        it('should query by id without keys: Error out', function(done) {
            User.find("", function(err, u) {
                should.not.exist(u);
                should.exist(err);
                done();
            });
        });

        it('should query by ids, not found, should be empty array', function(done) {
            var query = {};
            query.where = {};
            query.where.realm = "beatles";
            query.where.role = "leaders";
            query.where.order = "25";
            User.find(query, function(err, u) {
                should.exist([]);
                should.exist(u);
                should.not.exist(err);
                done();
            });
        });

        it('should query by id: found', function(done) {
            var query = {};
            query.where = {};
            query.where.realm = "beatles";
            query.where.id = "1";

            var aUser = [{
            realm: "beatles",
            id: '1',
            username: 'John Lennon',
            email: 'john@b3atl3s.co.uk',
            role: 'leaders',
            order: '2',
            tasks: 'Sing me a song'
            }];

            User.create(aUser, function(err, u) {
                should.not.exist(err);
                should.exist(u[0].id);
                should.exist(u[0].realm);
                User.find(query, function(err, u) {
                    should.exist(u);
                    should.not.exist(err);
                    u[0].should.be.an.instanceOf(User);
                    // u.destroy(function(err) {
                    done();
                    // });

                });
            });
        });

         it('should query by id: found', function(done) {
            var query = {};
            query.where = {};
            query.where.realm = "beatles";
            query.where.role = "leaders";
            query.where.order = "2";

            var aUser = [{
                realm: "beatles",
                id: '1',
                username: 'John Lennon',
                email: 'john@b3atl3s.co.uk',
                role: 'leaders',
                order: '2',
                tasks: 'Sing me a song'
            }];
        
            User.create(aUser, function(err, u) {
                should.not.exist(err);
                should.exist(u[0].id);
                User.find(query, function(err, u) {
                    should.exist(u[0]);
                    should.not.exist(err);
                    u[0].should.be.an.instanceOf(User);
                    u[0].destroy(function(err) {
                        done();
                    });

                });
            });
        });

    });

    describe('all', function() {
        before(seed);

        it('should query collection', function(done) {
            User.all({where: { realm: "beatles" }}, function(err, users) {
                should.exists(users);
                should.not.exists(err);
                users.should.have.lengthOf(6);
                done();
            });
        });

        it('should query limited collection', function(done) {
            User.all({
                where: { realm: "beatles" },
                limit: 3
            }, function(err, users) {
                should.exists(users);
                should.not.exists(err);
                users.should.have.lengthOf(3);
                done();
            });
        });

        it('should query offset collection with limit', function(done) {
            User.all({
                where: { realm: "beatles" },
                skip: 1,
                limit: 4
            }, function(err, users) {
                should.exists(users);
                should.not.exists(err);
                users.should.have.lengthOf(4);
                done();
            });
        });

        it('should query filtered collection', function(done) {
            User.all({
                where: {
                    realm: "beatles",
                    role: "leaders"
                }
            }, function(err, users) {
                should.exists(users);
                should.not.exists(err);
                users.should.have.lengthOf(2);
                done();
            });
        });

        it('should query collection sorted by numeric field', function(done) {
            User.all({
                 where: {
                    realm: "beatles"
                 },
                order: 'order'
            }, function(err, users) {
                should.exists(users);
                should.not.exists(err);
                users.forEach(function(u, i) {
                    u.order.should.eql(i + 1);
                });
                done();
            });
        });

        it('should query collection desc sorted by numeric field', function(done) {
            User.all({
                where: {
                    realm: "beatles"
                 },
                order: 'order DESC'
            }, function(err, users) {
                should.exists(users);
                should.not.exists(err);
                users.forEach(function(u, i) {
                    u.order.should.eql(users.length - i);
                });
                done();
            });
        });

        it('should query collection sorted by string field', function(done) {
            User.all({
                where: {
                    realm: "beatles"
                 },
                order: 'username'
            }, function(err, users) {
                should.exists(users);
                should.not.exists(err);
                users.shift().username.should.equal("George Harrison");
                users.shift().username.should.equal("John Lennon");
                users.pop().username.should.equal("Stuart Sutcliffe");
                done();
            });
        });

        it('should query collection desc sorted by string field', function(done) {
            User.all({
                where: {
                    realm: "beatles"
                 },
                order: 'username DESC'
            }, function(err, users) {
                should.exists(users);
                should.not.exists(err);
                users.pop().username.should.equal("George Harrison");
                users.pop().username.should.equal("John Lennon");
                users.shift().username.should.equal("Stuart Sutcliffe");
                done();
            });
        });

    });

    describe('count', function() {

        before(seed);

        it('should query total count', function(done) {
            User.count({
                    realm: "beatles"
                 }, function(err, n) {
                should.not.exist(err);
                should.exist(n);
                n.should.equal(6);
                done();
            });
        });

        it('should query filtered count', function(done) {
            User.count({
                realm: "beatles",
                role: 'leaders'
            }, function(err, n) {
                should.not.exist(err);
                should.exist(n);
                n.should.equal(2);
                done();
            });
        });
    });

    describe('findOne', function() {

        before(seed);

        it('should work even when find by id', function(done) {
            User.findOne(function(e, u) {
                User.findOne({
                    where: {
                        realm: "beatles",
                        id: "1",
                    }
                }, function(err, user) {
                    should.not.exist(err);
                    should.exist(user);
                    done();
                });
            });
        });

    });
});



describe('exists', function() {

    before(seed);

    it('should check whether record exist', function(done) {
        User.findOne(function(e, u) {
            User.exists("beatles", "1", function(err, exists) {
                should.not.exist(err);
                should.exist(exists);
                exists.should.be.ok;
                done();
            });
        });
    });

    it('should check whether record not exist', function(done) {
        User.destroyAll(function() {
            User.exists("badhash", "badsort", function(err, exists) {
                should.not.exist(err);
                exists.should.not.be.ok;
                done();
            });
        });
    });

});

function seed(done) {
    var count = 0;
    var beatles = [{
            realm: "beatles",
            id: '1',
            username: 'John Lennon',
            email: 'john@b3atl3s.co.uk',
            role: 'leaders',
            order: '2',
            tasks: 'Sing me a song'
        }, {
            realm: "beatles",
            id: '2',
            username: 'Paul McCartney',
            email: 'paul@b3atl3s.co.uk',
            role: 'leaders',
            order: '1',
            tasks: 'Play me a tune'
        },
        {
            realm: "beatles",
            id: '3',
            username: 'George Harrison',
            role: 'backer',
            order: '5'
        },
        {
            realm: "beatles",
            id: '4',
            username: 'Ringo Starr',
            role: 'backer',
            order: '6'
        },
        {
            realm: "beatles",
            id: '5',
            username: 'Pete Best',
            role: 'backer',
            order: '4'
        },
        {
            realm: "beatles",
            id: '6',
            username: 'Stuart Sutcliffe',
            role: 'backer',
            order: "3"
        }
    ];

    User.destroyAll( function() {
        beatles.forEach(function(beatle) {
            User.create(beatle, ok);
        });
    });


    function ok() {
        if (++count === beatles.length) {
            done();
        }
    }
}