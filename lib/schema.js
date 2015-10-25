var check = require('check-types'), 
	util = require('util'), 
	lodash = require('lodash'), 
	async = require('async');

function Schema(schema, connection, value) {
	if(!schema.fields) throw "Cassandra schema fields undefined";
	this._fields = schema.fields;
	this._connection = connection;
	this._value = {};
	this._udt = [];
	value = value || {};

	//	Setter/Getter
	Object.defineProperty(this, 'schemaName', {
	    get: function() { return this._name; },
	    set : function(name) { this._name = name }
	});

	Object.defineProperty(this, 'fields', {
	    get: function() { return this._fields; },
	    set : function(fields) { this._fields = fields }
	});

	Object.defineProperty(this, 'connection', {
	    get: function() { return this._connection; },
	    set : function(connection) { this._connection = connection }
	});

	Object.defineProperty(this, 'value', {
	    get: function() { return this._value; },
	    set : function(value) { this._value = value }
	});

	Object.defineProperty(this, 'udt', {
	    get: function() { return this._udt; },
	    set : function(udt) { this._udt = udt }
	});

	Object.defineProperty(this, 'primaryKey', {
	    get: function() {
	    	var primaryKeys = [];
	    	for(var fieldName in this._fields) {
	    		var fieldProperties = this._fields[fieldName];
	    		if(fieldProperties.primaryKey) primaryKeys.push(fieldName);
	    	}

	    	return primaryKeys;
	    },
	    set : function(connection) {
	    	throw Error('Unimplemented yet!')
	    }
	});

	var fieldNames = Object.keys(this._fields);
	for(var f=0; f<fieldNames.length; f++) {
		var fieldName = fieldNames[f];
		this._value[fieldName] = value[fieldName];

		Object.defineProperty(this, fieldName, {
		    get: function() { return this._value[fieldName]; },
		    set : function(value) { this._value[fieldName] = value }
		});
	}
}

Schema.prototype = {
	create : function(query, onSchemaCreated) {
		var Type = require('./type'), 
			self = this;

		var u = 0;
		function createTypeSync(onAllIterFinished) {
			if(self.udt.length > 0) {
				self.udt[u].connection = self.connection;
				Type.prototype.create.call(self.udt[u], function(err, res) {
					u++;

					if(u >= self.udt.length) onAllIterFinished(err, res);
					else createTypeSync(onAllIterFinished);
				});
			} else {
				onAllIterFinished();
			}
		}

		createTypeSync(function(err, res) {
			if(!self.connection) throw Error('Connection undefined');
			self.connection.executeDefinitionQuery(query, function(err, res) {
				if(onSchemaCreated) onSchemaCreated(err, res);
			})
		});
	}, 

	drop : function(query, onTableDropped) {
		var className = this.constructor;

		if(!this._connection) throw Error(util.format('%s : Connection undefined', className.name + '.drop'));
		this._connection.executeDefinitionQuery(query, function(err, res) {
			if(onTableDropped) onTableDropped(err, res)
		})
	}, 

	_buildCreateQueryFields : function() {
		var rows = [];
		var pks = [];
	    for(var fieldName in this._fields) {
	    	var fieldProperties = this._fields[fieldName];

	        if(typeof fieldProperties !== 'object') {
	        	throw Error('Field properties must be a JSON object')
	        } else if(!fieldProperties.type) {
	        	throw Error('Field must have \'type\' property')
	        }

	        if(fieldProperties.primaryKey) pks.push(fieldName);
	        rows.push(util.format('%s %s', fieldName, Schema.prototype._decodeType.call(this, fieldProperties.type)));
	    }
	    
	    if(pks.length > 0) rows.push( util.format('PRIMARY KEY ( %s )', pks.join(', ')) );

	    return rows.join(', ');
	}, 

	_decodeType : function(typeDef) {
		var Type = require('./type');
		
		if(check.string(typeDef)) {
			return typeDef;
		}
		else if(typeDef.constructor.super_ == Type) {
			this._udt.push(typeDef);
			//typeDef.create(this.constructor._cs);
			return 'frozen <' + typeDef.schemaName + '>';
		}
		else if(check.array(typeDef)) {
			//	LIST
			if(typeDef.length != 1) throw Error('Type \'LIST\' is not allowed having more than one elements');
			return 'list<'+ Schema._decodeType(typeDef[0]) +'>';
		}
		else if(check.object(typeDef)) {
			if(Object.keys(typeDef).length == 1) {
				//	SET
				throw Error('Type \'SET\' is not applied yet');
			}
			else if(Object.keys(typeDef).length == 2) {
				//	MAP
				sts = [];
				for(var i=0; i<=1; i++) {
					if(typeDef[i] instanceof Type) {
						// UDT
						sts[i] = typeDef[i].name;
					} else {
						// Another MAP
						sts[i] = Schema._decodeType(sts[i]);
					}
				}
				return 'map<' + sts.join(', ') + '>';
			}
			else throw Error('Type \'MAP\' is not allowed having more than two elements');
		}
	}
}

module.exports = Schema;