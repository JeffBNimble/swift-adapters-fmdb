//: Playground - noun: a place where people can play

import UIKit
import SwiftAdaptersFMDB

var str = "Hello, FMDBAdapters Playground"

var factory = FMDBDatabaseFactory()
var db = factory.createWithPath(nil)

try db.open()

var createStatement = "CREATE TABLE champion (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, title TEXT NOT NULL, blurb TEXT NOT NULL, key TEXT NOT NULL, image_url TEXT NOT NULL)"
var count = try db.executeUpdate(createStatement)
var cursor = try db.executeQuery("SELECT * FROM CHAMPION")
var cursorWrapper = cursor as! FMDBResultSetWrapper
cursor.next()

cursor.close()

createStatement = "INSERT INTO champion (id, name, title, blurb, key, image_url) values (1, \"Annie\", \"The marked assassin\", \"Whatevs\", \"A001\", \"http://datadragon.riotgames.com/images/A001.png\")"
count = try db.executeUpdate(createStatement)
cursor = try db.executeQuery("SELECT * FROM CHAMPION")
cursorWrapper = cursor as! FMDBResultSetWrapper
cursor.columnCount()
for index in 0..<cursor.columnCount() {
    cursor.columnNameFor(index)
}
cursor.next()
cursor.stringFor("name")
cursor.moveToLast()
cursor.intFor("id")
cursor.previous()
cursor.next()
cursor.next()
cursor.close()
