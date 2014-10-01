var pg = require('pg');
var Promise = require('bluebird');

function pluckId (row) {
	return +row[0].id;
}

function SqlError (args, error) {
	this.name = 'SqlError';
	this.args = args;
	this.details = error;
	this.message = 'Can\'t complete SQL query due to error.';
}

function mysqlToPg (sql) {
	var index = 1;
	return sql.replace(/\?/g, function () {
		return '$' + index++;
	});
}

function ctor (connectionParameters) {
	return function query () {
		var args = Array.prototype.slice.call(arguments);

		var isInsert = args[0].indexOf('insert into') == 0;

		if (isInsert) {
			args[0] += ' returning id';
		}

		args[0] = mysqlToPg(args[0]);

		var promise = new Promise(function(resolve, reject) {
			pg.connect(connectionParameters, function (error, client, done) {
				if (error) {
					return reject(new SqlError(args, error));
				}

				args.push(function (error, result) {
					
					done(); // Release connection to pool

					if (error) {
						reject(new SqlError(args, error));
					} else {
						resolve(result.rows);
					}
				});

				client.query.apply(client, args);
			});
		});

		if (isInsert) {
			promise = promise.then(pluckId);
		}

		return promise;
	};
}

module.exports = ctor;
