# most-data-sqlite
MOST Web Framework SQLite Adapter

##Install

$ npm install most-data-sqlite

##Usage

Register SQLite adapter on app.json as follows:

    "adapterTypes": [
        ...
          { "name":"SQLite Data Adapter", "invariantName": "sqlite", "type":"most-data-sqlite" }
        ...
        ],
    adapters: {
        ...
        "sqlite": { "name":"local-db", "invariantName":"sqlite", "default":true,
            "options": {
                database:"db/local.db"
            }
        ...
    }
}

If you are intended to use SQLite adapter as the default database adapter set the property "default" to true. 
