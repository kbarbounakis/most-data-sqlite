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
 * native extensions
 */
if (typeof Object.isNullOrUndefined !== 'function') {
    /**
     * Gets a boolean that indicates whether the given object is null or undefined
     * @param {*} obj
     * @returns {boolean}
     */
    Object.isNullOrUndefined = function(obj) {
        return (typeof obj === 'undefined') || (obj==null);
    }
}

/**
 * @class SqliteAdapter
 * @augments DataAdapter
 * @param {*} options
 * @constructor
 */
function SqliteAdapter(options) {
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

SqliteAdapter.prototype.open = function(callback) {
    var self = this;
    callback = callback || function() {};
    if (self.rawConnection) {
        callback();
    }
    else {
        //try to open or create database
        self.rawConnection = new sqlite3.Database(self.options.database,6, function(err) {
            if (err) {
                self.rawConnection = null;
            }
            callback(err);

        });
    }
};

SqliteAdapter.prototype.close = function(callback) {
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
SqliteAdapter.prototype.prepare = function(query,values) {
    return qry.prepare(query,values)
};

SqliteAdapter.formatType = function(field)
{
    var size = parseInt(field.size), s;
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
            return 'INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL';
        case 'Currency':
            s =  'NUMERIC(' + (field.size || 19) + ',4)';
            break;
        case 'Decimal':
            s =  'NUMERIC';
            if ((field.size) && (field.scale)) { s += '(' + field.size + ',' + field.scale + ')'; }
            break;
        case 'Date':
        case 'Time':
        case 'DateTime':
            s = 'NUMERIC';
            break;
        case 'Long':
        case 'Duration':
            s = 'INTEGER';
            break;
        case 'Integer':
            s = 'INTEGER' + (field.size ? '(' + field.size + ',0)':'' );
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
    if (field.primary) {
        return s.concat(' PRIMARY KEY NOT NULL');
    }
    else {
        return s.concat((field.nullable===undefined) ? ' NULL': (field.nullable ? ' NULL': ' NOT NULL'));
    }
};

/**
 * Begins a transactional operation by executing the given function
 * @param fn {function} The function to execute
 * @param callback {function(Error=)} The callback that contains the error -if any- and the results of the given operation
 */
SqliteAdapter.prototype.executeInTransaction = function(fn, callback) {
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
SqliteAdapter.prototype.createView = function(name, query, callback) {
    this.view(name).create(query, callback);
};


/*
 * @param {DataModelMigration|*} obj An Object that represents the data model scheme we want to migrate
 * @param {function(Error=)} callback
 */
SqliteAdapter.prototype.migrate = function(obj, callback) {
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
            result = result.replace(/%t/g,SqliteAdapter.formatType(obj));
        if (/%f/.test(format))
            result = result.replace(/%f/g,obj.name);
        return result;
    };


    async.waterfall([
        //1. Check migrations table existence
        function(cb) {
            if (SqliteAdapter.supportMigrations) {
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
                    SqliteAdapter.supportMigrations=true;
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

                var expressions = [],
                    /**
                     * @type {{columnName:string,ordinal:number,dataType:*, maxLength:number,isNullable:number,,primary:boolean }[]}
                     */
                    columns = args[1], forceAlter = false, column, newType, oldType;
                //validate operations

                //1. columns to be removed
                if (util.isArray(migration.remove)) {
                    if (migration.remove>0) {
                        for (var i = 0; i < migration.remove.length; i++) {
                            var x = migration.remove[i];
                            var colIndex = columns.indexOf(function(y) { return y.name== x.name; });
                            if (colIndex>=0) {
                                if (!columns[colIndex].primary) {
                                    forceAlter = true;
                                }
                                else {
                                    migration.remove.splice(i, 1);
                                    i-=1;
                                }
                            }
                            else {
                                migration.remove.splice(i, 1);
                                i-=1;
                            }
                        }
                    }
                }
                //1. columns to be changed
                if (util.isArray(migration.change)) {
                    if (migration.change>0) {

                        for (var i = 0; i < migration.change.length; i++) {
                            var x = migration.change[i];
                            column = columns.find(function(y) { return y.name==x.name; });
                            if (column) {
                                if (!column.primary) {
                                    //validate new column type (e.g. TEXT(120,0) NOT NULL)
                                    newType = format('%t', x); oldType = column.type.toUpperCase().concat(column.nullable ? ' NOT NULL' : ' NULL');
                                    if ((newType!=oldType)) {
                                        //force alter
                                        forceAlter = true;
                                    }
                                }
                                else {
                                    //remove column from change collection (because it's a primary key)
                                    migration.change.splice(i, 1);
                                    i-=1;
                                }
                            }
                            else {
                                //add column (column was not found in table)
                                migration.add.push(x);
                                //remove column from change collection
                                migration.change.splice(i, 1);
                                i-=1;
                            }

                        }

                    }
                }
                if (util.isArray(migration.add)) {

                    for (var i = 0; i < migration.add.length; i++) {
                        var x = migration.add[i];
                        column = columns.find(function(y) { return (y.name==x.name); });
                        if (column) {
                            if (column.primary) {
                                migration.add.splice(i, 1);
                                i-=1;
                            }
                            else {
                                newType = format('%t', x); oldType = column.type.toUpperCase().concat(column.nullable ? ' NOT NULL' : ' NULL');
                                if (newType==oldType) {
                                    //remove column from add collection
                                    migration.add.splice(i, 1);
                                    i-=1;
                                }
                                else {
                                    forceAlter = true;
                                }
                            }
                        }
                    }
                    if (forceAlter) {
                        cb(new Error('Full table migration is not yet implemented.'));
                        return;
                    }
                    else {
                        migration.add.forEach(function(x) {
                            //search for columns
                            expressions.push(util.format('ALTER TABLE "%s" ADD COLUMN "%s" %s', migration.appliesTo, x.name, SqliteAdapter.formatType(x)));
                        });
                    }

                }
                if (expressions.length>0) {
                    async.eachSeries(expressions, function(expr,cb) {
                        self.execute(expr, [], function(err) {
                            cb(err);
                        });
                    }, function(err) {
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
SqliteAdapter.prototype.selectIdentity = function(entity, attribute , callback) {

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
                        value = (parseInt(result[0][attribute]) || 0)+ 1;
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
SqliteAdapter.prototype.executeBatch = function(batch, callback) {
    callback = callback || function() {};
    callback(new Error('DataAdapter.executeBatch() is obsolete. Use DataAdapter.executeInTransaction() instead.'));
};

SqliteAdapter.prototype.table = function(name) {
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
         * @param {function(Error=,Array=)} callback
         */
        columns:function(callback) {
            callback = callback || function() {};
            self.execute('PRAGMA table_info(?)',
                [name], function(err, result) {
                    if (err) { callback(err); return; }
                    var arr = [];
                    /**
                     * enumerates table columns
                     * @param {{name:string},{cid:number},{type:string},{notnull:number},{pk:number}} x
                     */
                    var iterator = function(x) {
                        var col = { name: x.name, ordinal: x.cid, type: x.type,nullable: (x.notnull ? true : false), primary: (x.pk==1) };
                        var matches = /(\w+)\((\d+),(\d+)\)/.exec(x.type);
                        if (matches) {
                            //extract max length attribute (e.g. integer(2,0) etc)
                            if (parseInt(matches[2])>0) { col.size =  parseInt(matches[2]); }
                            //extract scale attribute from field (e.g. integer(2,0) etc)
                            if (parseInt(matches[3])>0) { col.scale =  parseInt(matches[3]); }
                        }
                        arr.push(col);
                    };
                    result.forEach(iterator);
                    callback(null, arr);
                });
        }
    }

};

SqliteAdapter.prototype.view = function(name) {
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
SqliteAdapter.prototype.execute = function(query, values, callback) {
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

SqliteAdapter.prototype.lastIdentity = function(callback) {
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
                    lastval = lastval || [];
                    if (lastval.length>0)
                        callback(null, { insertId:lastval[0]['lastval'] });
                    else
                        callback(null, { insertId: null });
                }
            });
        }
    });
};

function zeroPad(number, length) {
    number = number || 0;
    var res = number.toString();
    while (res.length < length) {
        res = '0' + res;
    }
    return res;
}

/**
 * @class SqliteFormatter
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

SqliteFormatter.NAME_FORMAT = '`$1`';

SqliteFormatter.prototype.escapeName = function(name) {
    if (typeof name === 'string')
        return name.replace(/(\w+)/ig, this.settings.nameFormat);
    return name;
};

var REGEXP_SINGLE_QUOTE=/\\'/g, SINGLE_QUOTE_ESCAPE ='\'\'',
    REGEXP_DOUBLE_QUOTE=/\\"/g, DOUBLE_QUOTE_ESCAPE = '"',
    REGEXP_SLASH=/\\\\/g, SLASH_ESCAPE = '\\';
/**
 * Escapes an object or a value and returns the equivalent sql value.
 * @param {*} value - A value that is going to be escaped for SQL statements
 * @param {boolean=} unquoted - An optional value that indicates whether the resulted string will be quoted or not.
 * returns {string} - The equivalent SQL string value
 */
SqliteFormatter.prototype.escape = function(value,unquoted)
{
    if (typeof value === 'boolean') { return value ? '1' : '0'; }
    if (value instanceof Date) {
        return this.escapeDate(value);
    }
    var res = SqliteFormatter.super_.prototype.escape.call(this, value, unquoted);
    if (typeof value === 'string') {
        if (REGEXP_SINGLE_QUOTE.test(res))
        //escape single quote (that is already escaped)
            res = res.replace(/\\'/g, SINGLE_QUOTE_ESCAPE);
        if (REGEXP_DOUBLE_QUOTE.test(res))
        //escape double quote (that is already escaped)
            res = res.replace(/\\"/g, DOUBLE_QUOTE_ESCAPE);
        if (REGEXP_SLASH.test(res))
        //escape slash (that is already escaped)
            res = res.replace(/\\\\/g, SLASH_ESCAPE);
    }
    return res;
};

/**
 * @param {Date|*} val
 * @returns {string}
 */
SqliteFormatter.prototype.escapeDate = function(val) {
    var year   = val.getFullYear();
    var month  = zeroPad(val.getMonth() + 1, 2);
    var day    = zeroPad(val.getDate(), 2);
    var hour   = zeroPad(val.getHours(), 2);
    var minute = zeroPad(val.getMinutes(), 2);
    var second = zeroPad(val.getSeconds(), 2);
    var millisecond = zeroPad(val.getMilliseconds(), 3);
    //format timezone
    var offset = val.getTimezoneOffset(),
        timezone = (offset<=0 ? '+' : '-') + zeroPad(-Math.floor(offset/60),2) + ':' + zeroPad(offset%60,2);
    return "'" + year + '-' + month + '-' + day + ' ' + hour + ':' + minute + ':' + second + "." + millisecond + timezone + "'";
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
 * Implements indexOf(str,substr) expression formatter.
 * @param {string} p0 The source string
 * @param {string} p1 The string to search for
 * @returns {string}
 */
SqliteFormatter.prototype.$indexOf = function(p0, p1)
{
    return util.format('(INSTR(%s,%s)-1)', this.escape(p0), this.escape(p1));
};

/**
 * Implements contains(a,b) expression formatter.
 * @param {string} p0 The source string
 * @param {string} p1 The string to search for
 * @returns {string}
 */
SqliteFormatter.prototype.$text = function(p0, p1)
{
    return util.format('(INSTR(%s,%s)-1)>=0', this.escape(p0), this.escape(p1));
};
/**
 * Implements simple regular expression formatter. Important Note: SQLite 3 does not provide a core sql function for regular expression matching.
 * @param {string|*} p0 The source string or field
 * @param {string|*} p1 The string to search for
 */
SqliteFormatter.prototype.$regex = function(p0, p1)
{
    //escape expression
    var s1 = this.escape(p1, true);
    //implement starts with equivalent for LIKE T-SQL
    if (/^\^/.test(s1)) {
        s1 = s1.replace(/^\^/,'');
    }
    else {
        s1 = '%' + s1;
    }
    //implement ends with equivalent for LIKE T-SQL
    if (/\$$/.test(s1)) {
        s1 = s1.replace(/\$$/,'');
    }
    else {
        s1 += '%';
    }
    return util.format('LIKE(\'%s\',%s) >= 1',s1, this.escape(p0));
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
 * Implements substring(str,pos) expression formatter.
 * @param {String} p0 The source string
 * @param {Number} pos The starting position
 * @param {Number=} length The length of the resulted string
 * @returns {string}
 */
SqliteFormatter.prototype.$substr = function(p0, pos, length)
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

SqliteFormatter.prototype.$day = function(p0) { return 'CAST(strftime(\'%d\', ' + this.escape(p0) + ') AS INTEGER)'; };
SqliteFormatter.prototype.$dayOfMonth = function(p0) { return 'CAST(strftime(\'%d\', ' + this.escape(p0) + ') AS INTEGER)'; };
SqliteFormatter.prototype.$month = function(p0) { return 'CAST(strftime(\'%m\', ' + this.escape(p0) + ') AS INTEGER)'; };
SqliteFormatter.prototype.$year = function(p0) { return 'CAST(strftime(\'%Y\', ' + this.escape(p0) + ') AS INTEGER)'; };
SqliteFormatter.prototype.$hour = function(p0) { return 'CAST(strftime(\'%H\', ' + this.escape(p0) + ') AS INTEGER)'; };
SqliteFormatter.prototype.$minute = function(p0) { return 'CAST(strftime(\'%M\', ' + this.escape(p0) + ') AS INTEGER)'; };
SqliteFormatter.prototype.$second = function(p0) { return 'CAST(strftime(\'%S\', ' + this.escape(p0) + ') AS INTEGER)'; };
SqliteFormatter.prototype.$date = function(p0) { return 'date(' + this.escape(p0) + ')'; };

var sqli = {
    /**
     * @constructs SqliteAdapter
     * */
    SqliteAdapter : SqliteAdapter,
    /**
     * @constructs SqliteFormatter
     * */
    SqliteFormatter : SqliteFormatter,
    /**
     * Creates an instance of SqliteAdapter object that represents a sqlite database connection.
     * @param {*} options An object that represents the properties of the underlying database connection.
     * @returns {DataAdapter|*}
     */
    createInstance: function(options) {
        return new SqliteAdapter(options);
    }
};

if (typeof exports !== 'undefined')
{
    module.exports = sqli;
}

