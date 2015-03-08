# most-data-sqlite
MOST Web Framework SQLite Adapter

##Install

$ npm install most-data-sqlite

##Usage

Register SQLite adapter on app.json as follows:

    adapters: {
        "sqlite": { "name":"local-db", "invariantName":"sqlite", "default":true,
            "options": {
              
            }
    }
}

If you are intended to use SQLite adapter as the default database adapter set the property "default" to true. 
