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

SQLiteAdapter.formatType = function(field)
{
    var size = parseInt(field.size);
    switch (field.type)
    {
        case 'Boolean':
            s = 'INTEGER(1,0)';
            break;
        case 'Byte':
            s = 'INTEGER(1,0)';
            break;
        case 'Number':
        case 'Float':
            s = 'REAL';
            break;
        case 'Counter':
            return 'INTEGER PRIMARY KEY AUTOINCREMENT';
        case 'Currency':
        case 'Decimal':
            s =  'NUMERIC';
            break;
        case 'Date':
        case 'Time':
        case 'DateTime':
            s = 'NUMERIC';
            break;
        case 'Long':
        case 'Integer':
        case 'Duration':
            s = 'INTEGER';
            break;
        case 'URL':
        case 'Text':
        case 'Note':
            s =field.size ? util.format('TEXT(%s,0)', field.size) : 'TEXT';
            break;
        case 'Image':
        case 'Binary':
            s ='BLOB';
            break;
        case 'Guid':
            s = 'TEXT(36,0)';
            break;
        case 'Short':
            s = 'INTEGER(2,0)';
            break;
        default:
            s = 'INTEGER';
            break;
    }
    s += field.nullable===undefined ? ' NULL': field.nullable ? ' NULL': ' NOT NULL';
    return s;
};

/**
 * Begins a transactional operation by executing the given function
 * @param fn {function} The function to execute
 * @param callback {function(Error=)} The callback that contains the error -if any- and the results of the given operation
 */
SQLiteAdapter.prototype.executeInTransaction = function(fn, callback) {
    var self = this;
    //ensure parameters
    fn = fn || function() {}; callback = callback || function() {};
    self.open(function(err) {
        if (err) {
            callback(err);
        }
        else {
            if (self.transaction) {
                fn.call(self, function(err) {
                    callback(err);
                });
            }
            else {
                //begin transaction
                self.rawConnection.run('BEGIN TRANSACTION;', undefined, function(err) {
                    if (err) {
                        callback(err);
                        return;
                    }
                    //initialize dummy transaction object (for future use)
                    self.transaction = { };
                    //execute function
                    fn.call(self, function(err) {
                        if (err) {
                            //rollback transaction
                            self.rawConnection.run('ROLLBACK;', undefined, function() {
                                self.transaction = null;
                                callback(err);
                            });
                        }
                        else {
                            //commit transaction
                            self.rawConnection.run('COMMIT;', undefined, function(err) {
                                self.transaction = null;
                                callback(err);
                            });
                        }
                    });
                });
            }
        }
    });
};

/**
 *
 * @param {string} name
 * @param {QueryExpression|*} query
 * @param {function(Error=)} callback
 */
SQLiteAdapter.prototype.createView = function(name, query, callback) {
    this.view(name).create(query, callback);
};


/*
 * @param {DataModelMigration|*} obj An Object that represents the data model scheme we want to migrate
 * @param {function(Error=)} callback
 */
SQLiteAdapter.prototype.migrate = function(obj, callback) {
    var self = this;
    callback = callback || function() {};
    if (typeof obj === 'undefined' || obj == null) { callback(); return; }
    /**
     * @type {DataModelMigration|*}
     */
    var migration = obj;

    var format = function(format, obj)
    {
        var result = format;
        if (/%t/.test(format))
            result = result.replace(/%t/g,SQLiteAdapter.formatType(obj));
        if (/%f/.test(format))
            result = result.replace(/%f/g,obj.name);
        return result;
    }


    async.waterfall([
        //1. Check migrations table existence
        function(cb) {
            if (SQLiteAdapter.supportMigrations) {
                cb(null, true);
                return;
            }
            self.table('migrations').exists(function(err, exists) {
                if (err) { cb(err); return; }
                cb(null, exists);
            });
        },
        //2. Create migrations table, if it does not exist
        function(arg, cb) {
            if (arg) { cb(null, 0); return; }
            //create migrations table
            self.execute('CREATE TABLE migrations("id" INTEGER PRIMARY KEY AUTOINCREMENT, ' +
                '"appliesTo" TEXT NOT NULL, "model" TEXT NULL, "description" TEXT,"version" TEXT NOT NULL)',
                [], function(err) {
                    if (err) { cb(err); return; }
                    SQLiteAdapter.supportMigrations=true;
                    cb(null, 0);
                });
        },
        //3. Check if migration has already been applied (true=Table version is equal to migration version, false=Table version is older from migration version)
        function(arg, cb) {
            self.table(migration.appliesTo).version(function(err, version) {
                if (err) { cb(err); return; }
                cb(null, (version>=migration.version));
            });
        },
        //4a. Check table existence (-1=Migration has already been applied, 0=Table does not exist, 1=Table exists)
        function(arg, cb) {
            //migration has already been applied (set migration.updated=true)
            if (arg) {
                migration['updated']=true;
                cb(null, -1);
            }
            else {
                self.table(migration.appliesTo).exists(function(err, exists) {
                    if (err) { cb(err); return; }
                    cb(null, exists ? 1 : 0);
                });

            }
        },
        //4. Get table columns
        function(arg, cb) {
            //migration has already been applied
            if (arg<0) { cb(null, [arg, null]); return; }
            self.table(migration.appliesTo).columns(function(err, columns) {
                if (err) { cb(err); return; }
                cb(null, [arg, columns]);
            });
        },
        //5. Migrate target table (create or alter)
        function(args, cb) {
            //migration has already been applied (args[0]=-1)
            if (args[0] < 0) {
                cb(null, args[0]);
            }
            else if (args[0] == 0) {
                //create table
                var strFields = migration.add.filter(function(x) {
                    return !x['oneToMany']
                }).map(
                    function(x) {
                        return format('"%f" %t', x);
                    }).join(', ');
                var sql = util.format('CREATE TABLE "%s" (%s)', migration.appliesTo, strFields);
                self.execute(sql, null, function(err) {
                    if (err) { cb(err); return; }
                    cb(null, 1);
                });
            }
            else if (args[0] == 1) {
                var expressions = [];
                //alter table
                if (util.isArray(migration.remove)) {
                    if (migration.remove>0) {
                        //todo::support drop column (rename table, create new and copy data)
                        cb(new Error('Drop column is not supported. This operation is not yet implemented.'));
                        return;
                    }
                }
                if (util.isArray(migration.change)) {
                    if (migration.change>0) {
                        //todo::support alter column (rename table, create new and copy data)
                        cb(new Error('Alter column is not supported. This operation is not yet implemented.'));
                        return;
                    }
                }
                if (util.isArray(migration.add)) {
                    migration.add.forEach(function(x) {
                        expressions.push(util.format('ALTER TABLE "%s" ADD COLUMN "%s" %s', migration.appliesTo, x.name, SQLiteAdapter.formatType(x)));
                    });
                }
                if (expressions.length>0) {
                    self.execute(expressions.join(';'), [], function(err)
                    {
                        if (err) { cb(err); return; }
                        cb(null, 1);
                    });
                }
                else {
                    cb(null, 2);
                }
            }
            else {
                cb(new Error('Invalid table status.'));
            }
        },
        function(arg, cb) {
            if (arg>0) {
                //log migration to database
                self.execute('INSERT INTO migrations("appliesTo", "model", "version", "description") VALUES (?,?,?,?)', [migration.appliesTo,
                    migration.model,
                    migration.version,
                    migration.description ], function(err, result) {
                    if (err)  {
                        cb(err);
                        return;
                    }
                    cb(null, 1);
                });
            }
            else {
                migration['updated'] = true;
                cb(null, arg);
            }
        }
    ], function(err) {
        callback(err);
    })

};

/**
 * Produces a new identity value for the given entity and attribute.
 * @param entity {String} The target entity name
 * @param attribute {String} The target attribute
 * @param callback {Function=}
 */
SQLiteAdapter.prototype.selectIdentity = function(entity, attribute , callback) {

    var self = this;

    var migration = {
        appliesTo:'increment_id',
        model:'increments',
        description:'Increments migration (version 1.0)',
        version:'1.0',
        add:[
            { name:'id', type:'Counter', primary:true },
            { name:'entity', type:'Text', size:120 },
            { name:'attribute', type:'Text', size:120 },
            { name:'value', type:'Integer' }
        ]
    }
    //ensure increments entity
    self.migrate(migration, function(err)
    {
        //throw error if any
        if (err) { callback.call(self,err); return; }
        self.execute('SELECT * FROM increment_id WHERE entity=? AND attribute=?', [entity, attribute], function(err, result) {
            if (err) { callback.call(self,err); return; }
            if (result.length==0) {
                //get max value by querying the given entity
                var q = qry.query(entity).select([qry.fields.max(attribute)]);
                self.execute(q,null, function(err, result) {
                    if (err) { callback.call(self, err); return; }
                    var value = 1;
                    if (result.length>0) {
                        value = parseInt(result[0][attribute]) + 1;
                    }
                    self.execute('INSERT INTO increment_id(entity, attribute, value) VALUES (?,?,?)',[entity, attribute, value], function(err) {
                        //throw error if any
                        if (err) { callback.call(self, err); return; }
                        //return new increment value
                        callback.call(self, err, value);
                    });
                });
            }
            else {
                //get new increment value
                var value = parseInt(result[0].value) + 1;
                self.execute('UPDATE increment_id SET value=? WHERE id=?',[value, result[0].id], function(err) {
                    //throw error if any
                    if (err) { callback.call(self, err); return; }
                    //return new increment value
                    callback.call(self, err, value);
                });
            }
        });
    });
};

/**
 * Executes an operation against database and returns the results.
 * @param {DataModelBatch} batch
 * @param {function(Error=)} callback
 */
SQLiteAdapter.prototype.executeBatch = function(batch, callback) {
    callback = callback || function() {};
    callback(new Error('DataAdapter.executeBatch() is obsolete. Use DataAdapter.executeInTransaction() instead.'));
};

SQLiteAdapter.prototype.table = function(name) {
    var self = this;
    return {
        /**
         * @param {function(Error,Boolean=)} callback
         */
        exists:function(callback) {
            self.execute('SELECT COUNT(*) count FROM sqlite_master WHERE name=? AND type=\'table\';', [name], function(err, result) {
                if (err) { callback(err); return; }
                callback(null, (result[0].count>0));
            });
        },
        /**
         * @param {function(Error,string=)} callback
         */
        version:function(callback) {
            self.execute('SELECT MAX(version) AS version FROM migrations WHERE appliesTo=?',
                [name], function(err, result) {
                    if (err) { cb(err); return; }
                    if (result.length==0)
                        callback(null, '0.0');
                    else
                        callback(null, result[0].version || '0.0');
                });
        },
        /**
         * @param {function(Error,Boolean=)} callback
         */
        has_sequence:function(callback) {
            callback = callback || function() {};
            self.execute('SELECT COUNT(*) count FROM sqlite_sequence WHERE name=?',
                [name], function(err, result) {
                    if (err) { callback(err); return; }
                    callback(null, (result[0].count>0));
                });
        },
        /**
         * @param {function(Error,{columnName:string,ordinal:number,dataType:*, maxLength:number,isNullable:number }[]=)} callback
         */
        columns:function(callback) {
            callback = callback || function() {};
            self.execute('PRAGMA table_info(?)',
                [name], function(err, result) {
                    if (err) { callback(err); return; }
                    var arr = [];
                    /**
                     * enumerates table columns
                     * @param {{name:string},{cid:number},{type:string},{notnull:number}} x
                     */
                    var iterator = function(x) {
                        arr.push({ columnName: x.name, ordinal: x.cid, dataType: x.type,isNullable: x.notnull ? true : false });
                    };
                    result.forEach(iterator);
                    callback(null, arr);
                });
        }
    }

};

SQLiteAdapter.prototype.view = function(name) {
    var self = this;
    return {
        /**
         * @param {function(Error,Boolean=)} callback
         */
        exists:function(callback) {
            self.execute('SELECT COUNT(*) count FROM sqlite_master WHERE name=? AND type=\'view\';', [name], function(err, result) {
                if (err) { callback(err); return; }
                callback(null, (result[0].count>0));
            });
        },
        /**
         * @param {function(Error=)} callback
         */
        drop:function(callback) {
            callback = callback || function() {};
            self.open(function(err) {
               if (err) { callback(err); return; }
                var sql = util.format("DROP VIEW IF EXISTS %s",name);
                self.execute(sql, undefined, function(err) {
                    if (err) { callback(err); return; }
                    callback();
                });
            });
        },
        /**
         * @param {QueryExpression|*} q
         * @param {function(Error=)} callback
         */
        create:function(q, callback) {
            var thisArg = this;
            self.executeInTransaction(function(tr) {
                thisArg.drop(function(err) {
                    if (err) { tr(err); return; }
                    try {
                        var sql = util.format("CREATE VIEW %s AS ",name);
                        var formatter = new SqliteFormatter();
                        sql += formatter.format(q);
                        self.execute(sql, undefined, tr);
                    }
                    catch(e) {
                        tr(e);
                    }
                });
            }, function(err) {
                callback(err);
            });

        }
    };
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
                //prepare statement - the traditional way
                var prepared = self.prepare(sql, values), params, fn;
                //validate statement
                if (/^(SELECT|PRAGMA)/ig.test(prepared)) {
                    //prepare for select
                    fn = self.rawConnection.all;
                }
                else {
                    //otherwise prepare for run
                    fn = self.rawConnection.run;
                }
                //execute raw command
                fn.call(self.rawConnection, prepared, [] , function(err, result) {
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

SQLiteAdapter.prototype.lastIdentity = function(callback) {
    var self = this;
    self.open(function(err) {
        if (err) {
            callback(err);
        }
        else {
            //execute lastval (for sequence)
            self.execute('SELECT last_insert_rowid() as lastval', [], function(err, lastval) {
                if (err) {
                    callback(null, { insertId: null });
                }
                else {
                    lastval.rows = lastval.rows || [];
                    if (lastval.rows.length>0)
                        callback(null, { insertId:lastval.rows[0]['lastval'] });
                    else
                        callback(null, { insertId: null });
                }
            });
        }
    });
};

/**
 * @class PGSqlFormatter
 * @constructor
 * @augments {SqlFormatter}
 */
function SqliteFormatter() {
    this.settings = {
        nameFormat:SqliteFormatter.NAME_FORMAT,
        forceAlias:true
    }
}
util.inherits(SqliteFormatter, qry.classes.SqlFormatter);

SqliteFormatter.NAME_FORMAT = '$1';

SqliteFormatter.prototype.escapeName = function(name) {
    if (typeof name === 'string')
        return name.replace(/(\w+)/ig, this.settings.nameFormat);
    return name;
};

/**
 * Escapes an object or a value and returns the equivalent sql value.
 * @param {*} value - A value that is going to be escaped for SQL statements
 * @param {boolean=} unquoted - An optional value that indicates whether the resulted string will be quoted or not.
 * returns {string} - The equivalent SQL string value
 */
SqliteFormatter.prototype.escape = function(value,unquoted)
{
    if (typeof value === 'boolean') { return value ? 1 : 0; }
    return SqliteFormatter.super_.prototype.escape.call(this, value, unquoted);
};

/**
 * Implements indexOf(str,substr) expression formatter.
 * @param {string} p0 The source string
 * @param {string} p1 The string to search for
 * @returns {string}
 */
SqliteFormatter.prototype.$indexof = function(p0, p1)
{
    return util.format('(INSTR(%s,%s)-1)', this.escape(p0), this.escape(p1));
};

/**
 * Implements concat(a,b) expression formatter.
 * @param {*} p0
 * @param {*} p1
 * @returns {string}
 */
SqliteFormatter.prototype.$concat = function(p0, p1)
{
    return util.format('(IFNULL(%s,\'\') || IFNULL(%s,\'\'))', this.escape(p0),  this.escape(p1));
};

/**
 * Implements substring(str,pos) expression formatter.
 * @param {String} p0 The source string
 * @param {Number} pos The starting position
 * @param {Number=} length The length of the resulted string
 * @returns {string}
 */
SqliteFormatter.prototype.$substring = function(p0, pos, length)
{
    if (length)
        return util.format('SUBSTR(%s,%s,%s)', this.escape(p0), pos.valueOf()+1, length.valueOf());
    else
        return util.format('SUBSTR(%s,%s)', this.escape(p0), pos.valueOf()+1);
};

/**
 * Implements length(a) expression formatter.
 * @param {*} p0
 * @returns {string}
 */
SqliteFormatter.prototype.$length = function(p0) {
    return util.format('LENGTH(%s)', this.escape(p0));
};

SqliteFormatter.prototype.$ceiling = function(p0) {
    return util.format('CEIL(%s)', this.escape(p0));
};

SqliteFormatter.prototype.$startswith = function(p0, p1)
{
    //validate params
    if (Object.isNullOrUndefined(p0) || Object.isNullOrUndefined(p1))
        return '';
    return 'LIKE(\'' + this.escape(p1, true) + '%\',' + this.escape(p0) + ')';
};

SqliteFormatter.prototype.$contains = function(p0, p1)
{
    //validate params
    if (Object.isNullOrUndefined(p0) || Object.isNullOrUndefined(p1))
        return '';
    return 'LIKE(\'%' + this.escape(p1, true) + '%\',' + this.escape(p0) + ')';
};

SqliteFormatter.prototype.$endswith = function(p0, p1)
{
    //validate params
    if (Object.isNullOrUndefined(p0) || Object.isNullOrUndefined(p1))
        return '';
    return 'LIKE(\'%' + this.escape(p1, true) + '\',' + this.escape(p0) + ')';
};

SqlFormatter.prototype.$day = function(p0) { return 'CAST(strftime(\'%d\', ' + this.escape(p0) + ') AS INTEGER)'; };
SqlFormatter.prototype.$month = function(p0) { return 'CAST(strftime(\'%m\', ' + this.escape(p0) + ') AS INTEGER)'; };
SqlFormatter.prototype.$year = function(p0) { return 'CAST(strftime(\'%Y\', ' + this.escape(p0) + ') AS INTEGER)'; };
SqlFormatter.prototype.$hour = function(p0) { return 'CAST(strftime(\'%H\', ' + this.escape(p0) + ') AS INTEGER)'; };
SqlFormatter.prototype.$minute = function(p0) { return 'CAST(strftime(\'%M\', ' + this.escape(p0) + ') AS INTEGER)'; };
SqlFormatter.prototype.$second = function(p0) { return 'CAST(strftime(\'%S\', ' + this.escape(p0) + ') AS INTEGER)'; };
SqlFormatter.prototype.$second = function(p0) { return 'date(' + this.escape(p0) + ')'; };

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
};

if (typeof exports !== 'undefined')
{
    module.exports = sqli;
}

