var Cql = require("cassandra-driver"), 
	Sync = require('sync'), 
	util = require('util'),
    check = require('check-types');

//	Constant
var DEFAULT_REPLICATION_FACTOR = 1;

var Cassandra = module.exports = function(config) {
	if(!config) throw Error("Cassandra connection configuration undefined");

	this._connectionOptions = config.connection;
	this._options = config.options;
    this._tables = {};

	if(!this._options.defaultReplicationStrategy) {
        this._options.defaultReplicationStrategy = {
            'class' : 'SimpleStrategy',
            'replication_factor' : DEFAULT_REPLICATION_FACTOR
        };
    }

    //  Setter/Getter
    Object.defineProperty(this, 'options', {
        get: function() { return this._options; },
        set : function(options) { this._options = options }
    });

    Object.defineProperty(this, 'tables', {
        get: function() { return this._tables; },
        set : function(tables) { this._tables = tables }
    });
}

Cassandra.prototype = {
	connect : function(onConnectFn) {
		var connOptions = this._connectionOptions;
		var options = this._options;
		var self = this;

		Sync(function() {
			self.createDatabase.sync(self);

			self._conn = new Cql.Client(connOptions);
			var query = util.format("USE %s;", connOptions.keyspace);
			self._conn.execute(query, function(err, res) {
				if(err) throw Error(err.message);
				if(typeof onConnectFn == 'function') onConnectFn(err, res)
			});
		});
	}, 

	createDatabase : function(onDbCreated) {
		var strReplication = this._buildReplicationString(this._options.defaultReplicationStrategy);
		var query = util.format("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = %s;",
            this._connectionOptions.keyspace, strReplication);

		var self = this;
		Sync(function() {
    		var conn = new Cql.Client({contactPoints : self._connectionOptions.contactPoints});
    		conn.execute(query, function(err, res) {
				if(err) throw Error(err.message);
				if(onDbCreated) onDbCreated(err, res)
			});
		});
	},

    //  Options : { eachRow : false , stream : false }
	executeQuery : function(query, options, onQueryExecuted) {
        var self = this, 
            args = Array.prototype.slice.call(arguments);

        if(check.function(args[1])) {
            onQueryExecuted = args[1];
            options = {};
        }

        var response = function(err, res) {
            if(err) {
                if(err.code == 8704) self.executeDefinitionQuery(query, onQueryExecuted)
                else onQueryExecuted(err.message);
            }
            onQueryExecuted(err, res);
        }.bind(this);

        var execute = function(query, params) {
            if(options.eachRow) {
                self._conn.eachRow(query, params, {'prepare': true}, function(n, rawRow) {
                    var row = {};
                    row[parseInt(n)] = rawRow;

                    response(null, row)
                }, function(err) {
                    response(err);
                });
            }
            else if(options.stream) {
                var stream = self._conn.stream(query, params, {'prepare': true});
                response(null, stream);
            }
            else {
                self._conn.execute(query, params, {'prepare': true}, response);
            }
        }

        if(check.string(query)) {
            params = [];
            execute(query, params);
        }
        else if(check.object(query)) {
            if(!query.query) throw Error('Parameter must contains key \'query\'');
            params = query.params || [];
            query = query.query;

            execute(query, params);
        }
        else if(check.array(query)) {
            query.forEach(function(q) {
                if(!q.query) throw Error('Parameter must contains key \'query\'');
                q.params = q.params || [];
            });

            self._conn.batch(query, {'prepare': true}, response);
        }
    }, 

    executeDefinitionQuery : function(query, onQueryExecuted) {
        var self = this;
        var response = function(err, res) {
            if(err) onQueryExecuted(err.message);
            onQueryExecuted(err, res);
        }

        if(check.string(query)) {
            params = [];
            self._conn.execute(query, {'prepare': false, 'fetchSize': 0}, response);
        }
        else if(check.object(query)) {
            if(!query.query) throw Error('Parameter must contains key \'query\'');
            params = query.params || [];
            query = query.query;

            self._conn.execute(query, {'prepare': false, 'fetchSize': 0}, response);
        }
        else if(check.array(query)) {
            query.forEach(function(q) {
                if(!q.query) throw Error('Parameter must contains key \'query\'');
                q.params = q.params || [];
            });

            self._conn.batch(query, {'prepare': false, 'fetchSize': 0}, response);
        }
    }, 

    _buildReplicationString : function(replication_option){
        if( typeof replication_option == 'string'){
            return replication_option;
        }else{
            var properties = [];
            for(var k in replication_option){
                properties.push(util.format("'%s': '%s'", k, replication_option[k] ));
            }
            return util.format('{%s}', properties.join(','));
        }
    },

    setModelsDir : function(dir) {
        var asMiddleware = function(req, res, next) {
            if(!req.cassandra) req.cassandra = {};
            req.cassandra.modelsDir = dir;

            next();
        }

        return asMiddleware;
    }, 

    asMiddleware : function(req, res, next) {
        if(!req.cassandra) req.cassandra = {};
        req.cassandra.connectionSource = this;

        next();
    }, 

    addOption : function(name, value) {
        this._options[name] = value;
    }, 

    getOption : function(name) {
        return this._options[name];
    }, 

    addTable : function(table) {
        var Table = require('./table');

        if(!table) throw Error('table cannot be null')
        else if(table.constructor.super_ != Table) throw Error('table Must be instance of CassandraTable');

        this._tables[table.name] = table;
    }, 

    getTable : function(name) {
        if(!name) throw Error('table name cannot be null');

        return this._tables[name];
    }
}

Object.defineProperty(Cassandra, "consistencies", {
    get: function() {
        return Cql.types.consistencies;
    }
});