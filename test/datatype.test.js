// This test written in mocha+should.js
var should = require("./init.js");

var db, Model;

describe("datatypes", function() {

    before(function(done){
        db = getSchema();
        Model = db.define("Model", {
            id: {
                type: String,
                id: 1,
                keyType: "hash"
            },
            str: String,
            num: Number,
            bool: Boolean
        });
        db.adapter.emitter.on("created-model", function() {
            Model.destroyAll(done);
        });
        done();
    });

    it("should keep types when get read data from db", function(done) {

        Model.create({
           id: "123", str: "hello", num: "3", bool: 1
        }, function(err, m) {
            should.not.exist(err);
            should.exist(m && m.id);
            m.str.should.be.a.String;
            m.num.should.be.a.Number;
            m.bool.should.be.a.Boolean;
            var realm = m.realm;
            var id = m.id;
            testFind(testAll);
        });

        function testFind(next) {
            var query = {};
            query.where = {};
            query.where.id = "123";

            Model.find(query, function(err, m) {
                should.not.exist(err);
                should.exist(m);
                m[0].str.should.be.a.String;
                m[0].num.should.be.a.Number;
                m[0].bool.should.be.a.Boolean;
                next();
            });
        }

        function testAll() {
            var query = {};
            query.where = {};
            query.where.id = "123";
            Model.findOne(query, function(err, m) {
                should.not.exist(err);
                should.exist(m);
                m.str.should.be.a.String;
                m.num.should.be.a.Number;
                m.bool.should.be.a.Boolean;
                done();
            });
        }

    });

    it("should convert \"false\" to false for boolean", function() {
        var m = new Model({bool: "false"});
        m.bool.should.equal(false);
    });

});
