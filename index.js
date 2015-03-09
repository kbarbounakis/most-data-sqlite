/**
 * Created by Kyriakos Barbounakis<k.barbounakis@gmail.com> on 26/11/2014.
 *
 * Copyright (c) 2014, Kyriakos Barbounakis k.barbounakis@gmail.com
 Anthi Oikonomou anthioikonomou@gmail.com
 All rights reserved.
 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this
 list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice,
 this list of conditions and the following disclaimer in the documentation
 and/or other materials provided with the distribution.
 * Neither the name of MOST Web Framework nor the names of its
 contributors may be used to endorse or promote products derived from
 this software without specific prior written permission.
 THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
var async = require('async'),
    util = require('util'),
    qry = require('most-query'),
    sqlite3 = require('sqlite3').verbose();
/**
 * @class SQLiteAdapter
 * @augments DataAdapter
 * @param {*} options
 * @constructor
 */
function SQLiteAdapter(options) {
    /**
     * @type {{database: string}}
     */
    this.options = options || { database: ':memory:' };
    /**
     * Represents the database raw connection associated with this adapter
     * @type {*}
     */
    this.rawConnection = null;
}

SQLiteAdapter.prototype.open = function(callback) {
    var self = this;
    callback = callback || function() {};
    if (self.rawConnection) {
        callback();
    }
    else {
        //try to open or create database
        self.rawConnection = new sqlite3.Database(self.options.database,6, function(err) {
            if (err) {
                self.rawConnection = null;;
            }
            callback(err);

        });
    }
};

SQLiteAdapter.prototype.close = function(callback) {
    var self = this;
    callback = callback || function() {};
    try {
        if (self.rawConnection)
        {
            //close connection
            self.rawConnection.close(function() {
                //and finally return
                callback();
            });
        }
        else {
            callback();
        }

    }
    catch (e) {
        console.log('An error occured while closing database.');
        console.log(e.message);
        //call callback without error
        callback();
    }
};

/**
 * @param {string} query
 * @param {*=} values
 */
SQLiteAdapter.prototype.prepare = function(query,values) {
    return qry.prepare(query,values)
};

/**
 * Executes a query against the underlying database
 * @param query {QueryExpression|string|*}
 * @param values {*=}
 * @param {function(Error=,*=)} callback
 */
SQLiteAdapter.prototype.execute = function(query, values, callback) {
    var self = this, sql = null;
    try {

        if (typeof query == 'string') {
            //get raw sql statement
            //todo: this operation may be obsolete (for security reasons)
            sql = query;
        }
        else {
            //format query expression or any object that may be act as query expression
            var formatter = new SqliteFormatter();
            sql = formatter.format(query);
        }
        //validate sql statement
        if (typeof sql !== 'string') {
            callback.call(self, new Error('The executing command is of the wrong type or empty.'));
            return;
        }
        //ensure connection
        self.open(function(err) {
            if (err) {
                callback.call(self, err);
            }
            else {
                //log statement (optional)
                if (process.env.NODE_ENV==='development')
                    console.log(util.format('SQL:%s, Parameters:%s', sql, JSON.stringify(values)));
                var prepared = self.prepare(sql, values), params, fn;
                //validate statement
                if (/^SELECT/ig.test(prepared)) {
                    //prepare for select
                    fn = self.rawConnection.all;
                }
                else {
                    //otherwise prepare for run
                    fn = self.rawConnection.run;
                }
                //execute raw command
                fn.call(self.rawConnection, prepared, params , function(err, result) {
                    if (err) {
                        //log sql
                        console.log(util.format('SQL Error:%s', prepared));
                        callback(err);
                    }
                    else {
                        if (result)
                            callback(null, result);
                        else
                            callback();
                    }
                });
            }
        });
    }
    catch (e) {
        callback.call(self, e);
    }

};

/**
 * @class PGSqlFormatter
 * @constructor
 * @augments {SqlFormatter}
 */
function SqliteFormatter() {
    this.settings = {
        nameFormat:SqliteFormatter.NAME_FORMAT
    }
}
util.inherits(SqliteFormatter, qry.classes.SqlFormatter);

SqliteFormatter.NAME_FORMAT = '$1';

var sqli = {
    /**
     * @constructs SQLiteAdapter
     * */
    SQLiteAdapter : SQLiteAdapter,
    /**
     * Creates an instance of SQLiteAdapter object that represents a sqlite database connection.
     * @param {*} options An object that represents the properties of the underlying database connection.
     * @returns {DataAdapter|*}
     */
    createInstance: function(options) {
        return new SQLiteAdapter(options);
    }
}

if (typeof exports !== 'undefined')
{
    module.exports = sqli;
}

