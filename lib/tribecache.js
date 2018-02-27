/*jshint esversion: 6 */
module.exports = function(settings) {
	var fs = require('fs');
	var parser = require("./parse.js");
	var sqlite3 = require('sqlite3').verbose();
	var Promise = require('bluebird');
	const path = require('path');
	var db = new sqlite3.cached.Database(settings.server_config.db_file);

	function initTable() {
		return new Promise((r, rj) => {
			db.run('CREATE TABLE IF NOT EXISTS tribes ( Id INTEGER  NOT NULL UNIQUE,Name VARCHAR NOT NULL,OwnerId INT NULL,	FileCreated DATETIME NULL,FileUpdated DATETIME NULL)', (err, sql) => {
				db.run('CREATE TABLE IF NOT EXISTS tribelogs ( Id INTEGER PRIMARY KEY AUTOINCREMENT, TribeId INT NOT NULL, Log VARCHAR NOT NULL )', (err, sql) => {
					r();
				});
			});
		});

	}

	function checkId(id) {
		return new Promise((r, rj) => {
			if (typeof id !== "number" || id < 1) {
				rj("Ignoring tribe with invalid id.");
			}
			db.get("SELECT id from tribes where id = " + id, (err, row) => {
				// console.log(row);
				r(row);
			});
		});
	}

	function saveTribes(data) {
		console.log("Setting up Tribes...");
		return new Promise((r, rj) => {
			db.run("BEGIN");
			let reqs = data.map((item) => {
				return new Promise((resolve) => {
					checkId(item.Id)
					.catch((reason) => {
						console.log(reason);
						resolve();
					})
					.then((d) => {
						if (d === undefined) {
							db.parallelize(function() {
								let qry = "INSERT INTO tribes (Id,Name,OwnerId,FileCreated,FileUpdated) VALUES (?,?,?,?,?)"; 
								db.run(qry, [item.Id, item.Name, item.OwnerId, item.FileCreated, item.FileUpdated],
										function(err, sql) {
									if (err) {
										console.log("Failed to insert new tribe:", err);
										console.log(qry);
										rj();
									}
								});
								// cn++;
								resolve();
							});
						} else {
							db.parallelize(function() {
								let qry = "UPDATE tribes SET OwnerId=?,FileUpdated=? WHERE Id=?";
								db.run(qry, [item.OwnerId, item.FileUpdated, item.Id],
										function(err, sql) {
									if (err) {
										console.log("Failed to update tribe:", err);
										console.log(qry);
										rj();
									}
								});
							});
							resolve();
						}
					})
				});
			});

			Promise.all(reqs).then(() => {
				db.run("COMMIT", () => {
					r();
				});
			});
		});
	}

	function purgeOldTribeLogs(id, timestamp) {
		return new Promise((r, rj) => {
			db.run("DELETE FROM tribelogs WHERE TribeId IN (SELECT Id FROM tribes WHERE Id = " + id + " AND FileUpdated < " + timestamp + " )", (err, sql) => {
				r();
			});
		});
	}

	function purgeTribeLogs() {
		return new Promise((r, rj) => {
			db.run("DROP TABLE tribelogs", (err, sql) => {
				r();
			});
		});
	}

	function saveTribeLogs(data) {
		console.log("Setting up TribeLogs...");
		return new Promise((r, rj) => {
			purgeTribeLogs()
			.then(initTable)
			.then(() => {
				db.run("BEGIN");
				let reqs = data.map((item) => {
					return new Promise((resolve) => {
						checkId(item.Id)
						.catch((reason) => {
							console.log(reason);
							resolve();
						})
						.then((d) => {
							if (d === undefined) {
								console.log("Ignoring tribelog for unknown tribe.");
								resolve();
							} else {
								item.Log.forEach(function(log) {
									if (log) {
										db.parallelize(function() {
											db.run("INSERT INTO tribelogs (Id,TribeId,Log) VALUES (NULL,?,?)", [item.Id, log],
													function(err, sql) {
												if (err) {
													console.log("Failed to insert tribelog:", err);
													rj();
												}
											});
											resolve();
										});
									} else {
										//console.log("Ignoring empty tribelog line.");
										resolve();
									}
								});
							}
						});
					});
				});

				Promise.all(reqs).then(() => {
					db.run("COMMIT", () => {
						r();
					});
				});
			});
		});
	}

	var qrylist = [];

	var readFilePromisified = Promise.promisify(require("fs").readFile);
	var readDirPromisified = Promise.promisify(require("fs").readdir);
	
	var module = {};
	module.setupTribes = function() {
		return new Promise((r, rj) => {
			initTable()
			.then(() => readDirPromisified(path.join(path.normalize(settings.server_config.ark_path), "ShooterGame", "Saved", "SavedArks"), "utf-8"))
			.then((files) => {
				var players = [];
				var tribeData = {};
				qrylist = [];
				let reqs = files.map((v) => {
					return new Promise(function(resolve) {
						var re = new RegExp("^.*\\.arktribe");
						if (re.test(v)) {
							var data = fs.readFileSync(path.join(path.normalize(settings.server_config.ark_path), "ShooterGame", "Saved", "SavedArks", v));
							tribeData = {};
							tribeData.Name = parser.getString("TribeName", data);
							tribeData.OwnerId = parser.getUInt32("OwnerPlayerDataID", data);
							tribeData.Id = parser.getInt("TribeID", data);
							tribeData.Log = parser.getStringArray('TribeLog', data);

							var fdata = fs.statSync(path.join(settings.server_config.ark_path, "ShooterGame", "Saved", "SavedArks", v));
							tribeData.FileCreated = new Date(fdata.birthtime);
							tribeData.FileUpdated = new Date(fdata.mtime);
							tribeData.FileCreated = tribeData.FileCreated.toISOString().slice(0, 19).replace('T', ' ');
							tribeData.FileUpdated = tribeData.FileUpdated.toISOString().slice(0, 19).replace('T', ' ');
							qrylist.push(tribeData);
						}
						resolve();
					});

				});
				Promise.all(reqs)
				.then(() => saveTribes(qrylist))
				.then(() => saveTribeLogs(qrylist))
				.then(() => r());
			}).catch(() => rj());
		});

	};
	
	return module;
};