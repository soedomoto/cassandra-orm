var util = require('util'), 
	check = require('check-types'), 
	Schema = require('./schema');

function Type(type) {
	if(!type.fields) throw "Cassandra Type structure undefined";
	Schema.apply(this, Array.prototype.slice.call(arguments));

	this.schemaName = type.constructor.name.replace('Type','').toLowerCase();
}

util.inherits(Type, Schema);
for(var i in Schema){
    if(Schema.hasOwnProperty(i)){
       Type[i] = Schema[i];
    }
}

Type.prototype = {
	create : function(options, onTypeCreated) {
		var args = Array.prototype.slice.call(arguments), 
			options = { drop : false, alter : false }, 
			query = [];

		if(args.length == 1) {
			if(check.object(args[0])) options = args[0];
			else if(check.function(args[0])) onTypeCreated = args[0];
		}

		var create = function(onTypeCreated) {
			var query = util.format(' CREATE TYPE IF NOT EXISTS %s ( %s ); ',
		        this.schemaName, Schema.prototype._buildCreateQueryFields.apply(this)
		    );

			try {
				Schema.prototype.create.call(this, query, function(err, res) {
					if(onTypeCreated) onTypeCreated(err, res)
				})
			} catch(err) {
				if(onTypeCreated) onTypeCreated(err)
			}
		}.bind(this);

		var alter = function(onTableAltered) {
			throw Error('Alter unimplemented');
		}.bind(this);

		if(options.drop) {
			this.drop(function(err) {
				if(!err) create(onTypeCreated);
			})
		} else {
			if(options.alter) {
				alter(onTypeCreated);
			} else {
				create(onTypeCreated);
			}
		}
	}, 

	drop : function(onTypeDropped) {
		var query = util.format(' DROP TYPE IF EXISTS %s; ', this.schemaName);

		try {
			Schema.prototype.drop.call(this, query, function(err, res) {
				if(onTypeDropped) onTypeDropped(err, res)
			})
		} catch(err) {
			if(onTypeDropped) onTypeDropped(err)
		}
	},
}

module.exports = Type;