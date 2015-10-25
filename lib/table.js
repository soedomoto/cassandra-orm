var util = require('util'), 
	lodash = require('lodash'),  
	check = require('check-types'), 
	Schema = require('./schema');

function Table(table) {
	if(!table.fields) throw "Cassandra Table structure undefined";
	Schema.apply(this, Array.prototype.slice.call(arguments));

	this.schemaName = table.constructor.name.replace('Table','').toLowerCase();
	this.create(function(err, res) {
		if(err) throw Error(err);
	});
}

util.inherits(Table, Schema);
for(var i in Schema){
    if(Schema.hasOwnProperty(i)){
       Table[i] = Schema[i];
    }
}

Table.prototype = {
	//	Options : { drop : (true/false), alter : (true/false) }
	create : function(options, onTableCreated) {
		var self = this, 
			args = Array.prototype.slice.call(arguments), 
			options = { drop : false, alter : false }, 
			query = [];

		if(args.length == 1) {
			if(check.object(args[0])) options = args[0];
			else if(check.function(args[0])) onTableCreated = args[0];
		}

		//	If cached, retrieve from cache
		if(this.connection.options['cache']) {
			var cacheTable = this.connection.getTable(this.schemaName);
			if(cacheTable) {
				for(var i in cacheTable){
				    if(cacheTable.hasOwnProperty(i)){
				       this[i] = cacheTable[i];
				    }
				}

				onTableCreated(null, {isCache : true});
				return;
			}
		}

		var create = function(onTableCreated) {
			var query = util.format(' CREATE TABLE IF NOT EXISTS %s ( %s ); ',
		        this.schemaName, Schema.prototype._buildCreateQueryFields.apply(this)
		    );

			try {
				Schema.prototype.create.call(this, query, function(err, res) {
					if(onTableCreated) onTableCreated(err, res)
				})
			} catch(err) {
				if(onTableCreated) onTableCreated(err)
			}
		};

		var alter = function(onTableAltered) {
			throw Error('Alter unimplemented');
		};

		if(options.drop) {
			this.drop(function(err) {
				if(!err) create.call(self, onTableCreated);
			})
		} else {
			if(options.alter) {
				alter.call(this, onTableCreated);
			} else {
				create.call(this, onTableCreated);
			}
		}
	}, 

	drop : function(onTableDropped) {
		var query = util.format(' DROP TABLE IF EXISTS %s; ', this.schemaName);

		try {
			Schema.prototype.drop.call(this, query, function(err, res) {
				if(onTableDropped) onTableDropped(err, res)
			})
		} catch(err) {
			if(onTableDropped) onTableDropped(err)
		}
	},

	//	Options : { eachRow : false , stream : false , raw : false }
	find : function(query, options, onRecordRetrieved) {
		var self = this, 
			className = self.constructor, 
			args = Array.prototype.slice.call(arguments);

		if(check.function(args[1])) {
            onRecordRetrieved = args[1];
            options = {};
        }

        if(check.object(query)) {
			if(!query.select) throw Error('Query must have \'select\' option');
			else if(check.array(query.select)) {
				var fields = Object.keys(self.fields), 
					selects = [];

				query.select.forEach(function(sel) {
					if(lodash.indexOf(fields, sel) == -1) {
						throw Error(util.format('Field \'%s\' does not exists in Table \'%s\'', sel, className.name))
					}
					selects.push(sel);
				})

				query.select = selects.join(', ');
			}

			if(query.where) {
				var fields = Object.keys(self.fields), 
					ws = Object.keys(query.where), 
					wheres = [], 
					params = [];

				for(var w=0; w<ws.length; w++) {
					if(lodash.indexOf(fields, ws[w]) == -1) {
						throw Error(util.format('Field \'%s\' does not exists in Table \'%s\'', ws[w], className.name))
					}
					wheres.push(ws[w] + '=?');
					params.push(query.where[ws[w]])
				}

				var where = 'WHERE ' + wheres.join(' AND ');
			}
			
			var q = util.format('SELECT %s FROM %s %s', 
						query.select, this.schemaName, where
					);
			if(options.allowFiltering) q += ' ALLOW FILTERING;'

			query = {};
			query.query = q;
			query.params = params;
		}
		else if(check.string(query)) {
			if(options.allowFiltering) query += ' ALLOW FILTERING;'
		}

		if(options.eachRow) {
			self.connection.executeQuery(query, options, function(err, res) {
				if(onRecordRetrieved) {
					if(options.raw) {
						if(err || res) onRecordRetrieved(err, res);
					}
					else if(res) {
						var keys = Object.keys(res);
						for(var k=0; k<keys.length; k++) {
							res[keys[k]] = new className(self.connection, res[keys[k]]);
						}
						onRecordRetrieved(null, res)
					}
				}
			})
		} 
		else if(options.stream) {
			self.connection.executeQuery(query, options, function(err, res) {
				if(onRecordRetrieved) {
					res.on('error', function (err) {
						onRecordRetrieved(err);
					});

					res.on('readable', function () {
						while (row = this.read()) {
							if(options.raw) onRecordRetrieved(null, row);
							else {
								row = new className(self.connection, row);
								onRecordRetrieved(null, row);
							}
						}
					});
				}
			})
		}
		else {
			self.connection.executeQuery(query, function(err, res) {
				if(onRecordRetrieved) {
					if(err) onRecordRetrieved(err.message);
					else if(options.raw) onRecordRetrieved(err, res);
					else {
						if(res.rows) {
							for(var r=0; r<res.rows.length; r++) {
								res.rows[r] = new className(self.connection, res.rows[r]);
							}
							onRecordRetrieved(null, res.rows)
						}
					}
				}
			})
		}
	}, 

	insert : function(value, onRecordInserted) {
		var self = this, 
			args = Array.prototype.slice.call(arguments), 
			values = [];

		if(args.length > 1) {
			if(check.function(args[0])) onRecordInserted = args[0];
			else if(check.object(value)) {
				values.push(lodash.extend(this._value, value));
			} 
			else if(check.array(value)) {
				value.forEach(function(v) {
					if(check.object(v)) {
						values.push(lodash.extend(self._value, v))
					}
				})
			}
			else throw Error('Value parameter must be an object or an array of objects. Object keys must be associated with fields')
		} 

		if(values.length == 0) {
			values.push(this._value);
		}

		var query = [];
		values.forEach(function(val) {
			var keys = Object.keys(self.fields), 
				insertVal = [], 
				bracketVal = [];

			for(var k=0; k<keys.length; k++) {
				bracketVal.push('?')
				insertVal.push(val[keys[k]]);
			}

			var q = util.format(' INSERT INTO %s ( %s ) VALUES ( %s ); ', 
						self.schemaName, Object.keys(self._fields).join(', '), bracketVal.join(', ')
					);
			query.push( { query : q, params : insertVal } );
		})

		self.connection.executeQuery(query, function(err, res) {
			if(onRecordInserted) onRecordInserted(err, res)
		})
	}, 

	update : function(value, onRecordUpdated) {
		var self = this, 
			className = self.constructor, 
			args = Array.prototype.slice.call(arguments);

		if(!check.object(value)) throw Error(util.format('%s : First argument must be an object contains new value', className.name + '.update'));
		if(Object.keys(value).length == 0) throw Error(util.format('%s : First argument must contains element', className.name + '.update'));

		var upds = Object.keys(value), 
			fields = Object.keys(this._fields), 
			updates = [], 
			wheres = [], 
			params = [];
		
		for(var u=0; u<upds.length; u++) {
			if(lodash.indexOf(fields, upds[u]) == -1) {
				throw Error(util.format('%s : Field \'%s\' does not exists in Table \'%s\'', className.name + '.update', upds[u], className.name))
			}

			updates.push(upds[u] + '=?');
			params.push(value[upds[u]]);
		}

		this.primaryKey.forEach(function(pk) {
			if(!self._value[pk]) throw Error(util.format('%s : Primary key \'%s\' value cannot be null', className.name + '.update', pk));
			wheres.push(pk + '=?');
			params.push(self._value[pk]);
		});

		var q = util.format(' UPDATE %s SET %s WHERE %s; ', 
					self.schemaName, updates.join(', '), wheres.join(' AND ')
				);
		self.connection.executeQuery({ query : q, params : params }, function(err, res) {
			if(!err) {
				self._value = lodash.extend(self._value, value);
				res = self;
			}

			if(onRecordUpdated) onRecordUpdated(err, res)
		})
	}, 

	delete : function (value, onRecordDeleted) {
		var self = this, 
			className = self.constructor, 
			args = Array.prototype.slice.call(arguments);

		if(check.function(args[0])) onRecordDeleted = args[0];
		this.value = lodash.extend(this.value, value);

		var wheres = [], 
			params = [];

		this.primaryKey.forEach(function(pk) {
			if(!self._value[pk]) throw Error(util.format('%s : Primary key \'%s\' value cannot be null', className.name + '.delete', pk));
			wheres.push(pk + '=?');
			params.push(self._value[pk]);
		});

		var q = util.format(' DELETE FROM %s WHERE %s; ', 
					self.schemaName, wheres.join(' AND ')
				);
		self.connection.executeQuery({ query : q, params : params }, function(err, res) {
			if(onRecordDeleted) onRecordDeleted(err, res)
		})
	}
}

module.exports = Table;