//: Playground - noun: a place where people can play

import UIKit
import SwiftAdaptersFMDB
import SwiftProtocolsSQLite
import CocoaLumberjackSwift

var str = "Hello, FMDBAdapters Playground"

DDLog.addLogger(DDTTYLogger.sharedInstance())
defaultDebugLevel = .Verbose

/// Create the FMDB database factory and an in-memory database
var factory = FMDBDatabaseFactory()
var db : SQLiteDatabase = try factory.create()

/// Open it, create a table and insert some rows
db.isOpen
try db.open()
db.isOpen

var createStatement = "CREATE TABLE champion (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, title TEXT NOT NULL, blurb TEXT NOT NULL, key TEXT NOT NULL, image_url TEXT NOT NULL)"
try db.executeUpdate(createStatement)

var insertStatement = "INSERT INTO champion (id, name, title, blurb, key, image_url) values (1, \"Annie\", \"The marked assassin\", \"Whatevs\", \"A001\", \"http://datadragon.riotgames.com/images/A001.png\")"
try db.executeUpdate(insertStatement)
insertStatement = "INSERT INTO champion (id, name, title, blurb, key, image_url) values (2, \"Alistar\", \"Bad Mamma Jamma\", \"Mean, mean\", \"A002\", \"http://datadragon.riotgames.com/images/A002.png\")"
try db.executeUpdate(insertStatement)
insertStatement = "INSERT INTO champion (id, name, title, blurb, key, image_url) values (3, \"Ziggs\", \"Bombs away!!\", \"TNT\", \"A003\", \"http://datadragon.riotgames.com/images/A003.png\")"
try db.executeUpdate(insertStatement)

// Execute a query and get back a cursor
var cursor = try db.executeQuery("SELECT * FROM champion ORDER BY name asc")

// Loop through and print out some data
while cursor.next() {
    var id = cursor.intFor("id")
    var name = cursor.stringFor("name")
    var title = cursor.stringFor("title")
    print("Champion \(id) named \(name) - \(title)")
}

/// Now, move around and make sure all is good
cursor.moveToFirst()
cursor.stringFor("name") == "Alistar"
cursor.moveToLast()
cursor.stringFor("name") == "Ziggs"
cursor.move(-1)
cursor.stringFor("name") == "Annie"
cursor.moveToPosition(0)
cursor.stringFor("name") == "Alistar"
cursor.moveToPosition(2)
cursor.stringFor("name") == "Ziggs"

// Now, lets use a SQLQueryOperation to run some queries
var statementBuilder:SQLStatementBuilder = SQLiteStatementBuilder()
var queryOperation = SQLQueryOperation(database:db, statementBuilder:statementBuilder)

// Build a similar champion query
queryOperation.tableName = "champion"
queryOperation.projection = ["id", "name", "title"]
queryOperation.sort = "name asc"

var opCursor = try queryOperation.executeQuery()

// Loop through this cursor and print out the data
while opCursor.next() {
    var name = opCursor.stringFor("name")
    print("I found \(name)")
}

// Now, use a SQLUpdateOperation to delete a row using named parameters
var namedUpdateOperation = SQLUpdateOperation(database: db, statementBuilder: statementBuilder)
namedUpdateOperation.tableName = "champion"
namedUpdateOperation.selection = "name = :name"
namedUpdateOperation.namedSelectionArgs = ["name":"Annie"]

try namedUpdateOperation.executeDelete()

// Now, execute a count query using another query operation to verify that I have only 2 rows left
var countQueryOperation = SQLQueryOperation(database:db, statementBuilder:statementBuilder)
countQueryOperation.tableName = "champion"
countQueryOperation.projection = ["count(*)"]

var countCursor = try countQueryOperation.executeQuery()
countCursor.next()
countCursor.intFor(0)

// Update Alistar using a named update operation
var updateOperation = SQLUpdateOperation(database: db, statementBuilder: statementBuilder)
updateOperation.tableName = "champion"
updateOperation.selection = "name = :name"
updateOperation.namedSelectionArgs = ["name":"Alistar"]
updateOperation.contentValues = ["title":"Mean, Mean, Jelly Bean!"]
try updateOperation.executeUpdate()

// Finally, query Alistar to make sure he got updated
queryOperation.selection = "name = :name"
queryOperation.namedSelectionArgs = ["name":"Alistar"]

countCursor = try queryOperation.executeQuery()
countCursor.next()
countCursor.stringFor("title") == "Mean, Mean, Jelly Bean!"
