//
// Created by Jeff Roberts on 7/18/15.
// Copyright (c) 2015 nimbleNoggin.io. All rights reserved.
//

import Foundation
import SwiftProtocolsSQLite
import fmdbframework

public class FMDBDatabaseFactory:SQLiteDatabaseFactory {
    public required init() {}
    
    public func createWithPath(absolutePath:String?) -> SQLiteDatabase {
        return FMDBDatabaseWrapper(path:absolutePath)
    }
}

public class FMDBDatabaseWrapper:SQLiteDatabase {
    internal var fmdatabase:FMDatabase
    
    public required init(path:String?) {
        self.fmdatabase = FMDatabase(path: path)
    }
    
    /// changes: The number of rows inserted/updated/deleted from the last successful SQL statement
    public var changes:Int {
        get {
            return Int(self.fmdatabase.changes())
        }
    }
    
    /// isOpen: A boolean indicating whether the database is currently open
    public var isOpen:Bool {
        get {
            return self.fmdatabase.open()
        }
    }
    
    /// lastInsertedRowId: The unique id of the row last inserted
    public var lastInsertedRowId:Int {
        get {
            return Int(self.fmdatabase.lastInsertRowId())
        }
    }
    
    /// path: THe absolute path to the sqlite3 database file
    public var path:String? {
        get {
            return self.fmdatabase.databasePath()
        }
    }
    
    /// open: Open the database
    /// Returns: A boolean indicating whether or not the database was successfully opened
    /// Throws: A SQLiteDatabaseError if the database could not be opened
    public func open() throws -> Bool {
        let wasOpened = self.fmdatabase.open()
        guard self.fmdatabase.hadError() else {
            return wasOpened
        }
        
        throw self.fmdatabase.lastError()
    }
    
    /// close: Close the database
    /// Returns: A boolean indicating whether or not the database was successfully closed
    /// THrows: A SQLiteDatabaseError if the database could not be opened
    public func close() throws -> Bool {
        let wasClosed = self.fmdatabase.close()
        guard self.fmdatabase.hadError() else {
            return wasClosed
        }
        
        throw self.fmdatabase.lastError()
    }
    
    /// startTransaction: Starts a database transaction
    /// Returns: A boolean indicating whether or not a database transaction was started
    /// Throws: A SQLiteDatabaseError if the transaction was not started
    public func startTransaction() throws -> Bool {
        let wasStarted = self.fmdatabase.beginTransaction()
        guard self.fmdatabase.hadError() else {
            return wasStarted
        }
        
        throw self.fmdatabase.lastError()
    }
    
    /// commit: Commits an existing database transaction
    /// Returns: A boolean indicating whether or not the commit was successful
    /// Thows: A SQLiteDatbaseError if the transaction was not committed
    public func commit() throws -> Bool {
        let wasCommitted = self.fmdatabase.commit()
        guard self.fmdatabase.hadError() else {
            return wasCommitted
        }
        
        throw self.fmdatabase.lastError()
    }
    
    /// rollback: Rolls back an existing database transaction
    /// Returns: A boolean indicting whether or not the transaction as successfully rolled back
    /// Throws: A SQLiterDatabaseError if the transation was not rolled bak
    public func rollback() throws -> Bool {
        let wasRolledBack = self.fmdatabase.rollback()
        guard self.fmdatabase.hadError() else {
            return wasRolledBack
        }
        
        throw self.fmdatabase.lastError()
    }
    
    /// executeUpdate: Executes a SQL statement that updates the database in some way
    /// Parameter sqlString: THe SQL string to execute
    /// Returns: The number of rows inserted/updated/deleted
    /// THrows: A SQLiteDatbaseError if the statement fails
    public func executeUpdate(sqlString: String) throws -> Int {
        self.fmdatabase.executeUpdate(sqlString, withArgumentsInArray: [AnyObject]())
        guard self.fmdatabase.hadError() else {
            return self.changes
        }
        
        throw self.fmdatabase.lastError()
    }
    
    /// executeUpdate: Executes a SQL statement that updates the database in some way
    /// Parameter sqlString: THe SQL string to execute
    /// Parameter parameters: An Array of parameters that are bound to parameter markers in the SQL
    /// Returns: The number of rows inserted/updated/deleted
    /// THrows: A SQLiteDatbaseError if the statement fails
    public func executeUpdate(sqlString: String, parameters:[AnyObject]?) throws -> Int {
        self.fmdatabase.executeUpdate(sqlString, withArgumentsInArray: parameters)
        guard self.fmdatabase.hadError() else {
            return self.changes
        }
        
        throw self.fmdatabase.lastError()
    }
    
    /// executeUpdate: Executes a SQL statement that updates the database in some way
    /// Parameter sqlString: THe SQL string to execute
    /// Parameter parameters: A Dictionary of parameters that are bound to bind variables in the SQL
    /// Returns: The number of rows inserted/updated/deleted
    /// THrows: A SQLiteDatbaseError if the statement fails
    public func executeUpdate(sqlString: String, parameters:[String:AnyObject]?) throws -> Int {
        self.fmdatabase.executeUpdate(sqlString, withParameterDictionary: parameters)
        guard self.fmdatabase.hadError() else {
            return self.changes
        }
        
        throw self.fmdatabase.lastError()
    }
    
    /// executeQuery: Executes a SQL query statement that returns zero or more rows
    /// Parameter sqlString: THe SQL string to execute
    /// Returns: A cursor containing the query results
    /// THrows: A SQLiteDatbaseError if the statement fails
    public func executeQuery(sqlString: String) throws -> Cursor {
        let resultSet = self.fmdatabase.executeQuery(sqlString, withArgumentsInArray:[AnyObject]())
        guard self.fmdatabase.hadError() else {
            return FMDBResultSetWrapper(resultSet: resultSet)
        }
        
        throw self.fmdatabase.lastError()
    }
    
    /// executeQuery: Executes a SQL query statement that returns zero or more rows
    /// Parameter sqlString: THe SQL string to execute
    /// Parameter parameters: An Array of parameters that are bound to parameter markers in the SQL
    /// Returns: The number of rows inserted/updated/deleted
    /// THrows: A SQLiteDatbaseError if the statement fails
    public func executeQuery(sqlString: String, parameters:[AnyObject]?) throws -> Cursor {
        let resultSet = self.fmdatabase.executeQuery(sqlString, withArgumentsInArray:parameters)
        guard self.fmdatabase.hadError() else {
            return FMDBResultSetWrapper(resultSet: resultSet)
        }
        
        throw self.fmdatabase.lastError()
    }
    
    /// executeQuery: Executes a SQL query statement that returns zero or more rows
    /// Parameter sqlString: THe SQL string to execute
    /// Parameter parameters: A Dictionary of parameters that are bound to bind variables in the SQL
    /// Returns: The number of rows inserted/updated/deleted
    /// THrows: A SQLiteDatbaseError if the statement fails
    public func executeQuery(sqlString: String, parameters:[String:AnyObject]?) throws -> Cursor {
        let resultSet = self.fmdatabase.executeQuery(sqlString, withParameterDictionary:parameters)
        guard self.fmdatabase.hadError() else {
            return FMDBResultSetWrapper(resultSet: resultSet)
        }
        
        throw self.fmdatabase.lastError()
    }
    
}

public class FMDBResultSetWrapper:Cursor {
    /// A cached array of column names in column index order
    private var columnNames = [String]()
    
    // The current 0-based cursor position
    private var cursorPosition = -1
    
    // The wrapped FMDB FMResultSet
    private var fmResultSet:FMResultSet
    
    // An Array of dictionaries (1 per row) containing the rows cached (which are cached upon access)
    var rowCache:[[String:AnyObject]]
    
    public required init(resultSet:FMResultSet) {
        self.fmResultSet = resultSet
        
        // Initialize properties
        self.rowCache = []
        self.initializedColumnNames()
        
        // Pre-cache some rows
        self.ensureRowCacheUpTo(4) // Cache the first chunk of rows
    }
    
    public func boolFor(columnName:String) -> Bool {
        return self.fmResultSet.boolForColumn(columnName)
    }
    
    public func boolFor(columnIndex:Int) -> Bool {
        return self.fmResultSet.boolForColumnIndex(Int32(columnIndex))
    }
    
    public func close() -> Void {
        return self.fmResultSet.close()
    }
    
    public func columnCount() -> Int {
        return Int(self.fmResultSet.columnCount())
    }
    
    public func columnIndexFor(columnName:String) -> Int {
        return Int(self.fmResultSet.columnIndexForName(columnName))
    }
    
    public func columnAtIndexIsNull(columnIndex:Int) -> Bool {
        return self.fmResultSet.columnIndexIsNull(Int32(columnIndex))
    }
    
    public func columnIsNull(columnName:String) -> Bool {
        return self.fmResultSet.columnIsNull(columnName)
    }
    
    public func columnNameFor(columnIndex:Int) -> String {
        return self.fmResultSet.columnNameForIndex(Int32(columnIndex))
    }
    
    public func dataFor(columnName:String) -> NSData {
        return self.fmResultSet.dataForColumn(columnName)
    }
    
    public func dataFor(columnIndex:Int) -> NSData {
        return self.fmResultSet.dataForColumnIndex(Int32(columnIndex))
    }
    
    public func dateFor(columnName:String) -> NSDate {
        return self.fmResultSet.dateForColumn(columnName)
    }
    
    public func dateFor(columnIndex:Int) -> NSDate {
        return self.fmResultSet.dateForColumnIndex(Int32(columnIndex))
    }
    
    public func doubleFor(columnName:String) -> Double {
        return self.fmResultSet.doubleForColumn(columnName)
    }
    
    public func doubleFor(columnIndex:Int) -> Double {
        return self.fmResultSet.doubleForColumnIndex(Int32(columnIndex))
    }
    
    public func intFor(columnName:String) -> Int {
        return Int(self.fmResultSet.intForColumn(columnName))
    }
    
    public func intFor(columnIndex:Int) -> Int {
        return Int(self.fmResultSet.intForColumnIndex(Int32(columnIndex)))
    }
    
    public func longFor(columnName:String) -> Int32 {
        return self.fmResultSet.intForColumn(columnName)
    }
    
    public func longFor(columnIndex:Int) -> Int32 {
        return Int32(self.fmResultSet.longForColumnIndex(Int32(columnIndex)))
    }
    
    public func longLongIntFor(columnName:String) -> Int64 {
        return Int64(self.fmResultSet.longLongIntForColumn(columnName))
    }
    
    public func longLongIntFor(columnIndex:Int) -> Int64 {
        return Int64(self.fmResultSet.longLongIntForColumnIndex(Int32(columnIndex)))
    }
    
    public func moveTo(offset:Int) -> Bool {
        guard self.ensureRowCacheUpTo(self.cursorPosition + offset) else {
            return false
        }
        
        self.cursorPosition += offset
        return true
    }
    
    public func moveToFirst() -> Bool {
        guard self.ensureRowCacheUpTo(1) else {
            return false
        }
        
        self.cursorPosition = 0
        return true
    }
    
    public func moveToLast() -> Bool {
        self.ensureRowCacheUpTo(Int.max)
        
        self.cursorPosition = self.rowCache.count
        return true
    }
    
    public func moveToPosition(absolutePosition:Int) -> Bool {
        guard self.ensureRowCacheUpTo(absolutePosition) else {
            return false
        }
        
        self.cursorPosition = absolutePosition
        return true
    }
    
    public func next() -> Bool {
        return self.moveTo(1)
    }
    
    public func previous() -> Bool {
        return self.moveTo(-1)
    }
    
    public func stringFor(columnName:String) -> String {
        return self.fmResultSet.stringForColumn(columnName)
    }
    
    public func stringFor(columnIndex:Int) -> String {
        return self.fmResultSet.stringForColumnIndex(Int32(columnIndex))
    }
    
    public func unsignedLongLongIntFor(columnName:String) -> UInt64 {
        return self.fmResultSet.unsignedLongLongIntForColumn(columnName)
    }
    
    public func unsignedLongLongIntFor(columnIndex:Int) -> UInt64 {
        return self.fmResultSet.unsignedLongLongIntForColumnIndex(Int32(columnIndex))
    }
    
    private func ensureRowCacheUpTo(cachePosition:Int) -> Bool {
        guard self.rowCache.count < self.cursorPosition else {
            return true
        }
        
        for _ in 0...cachePosition {
            guard self.next() else {
                return false
            }
            
            self.rowCache.append(self.fmResultSet.resultDictionary() as! [String:AnyObject])
        }
    
        return true
    }

    private func initializedColumnNames() {
        for index in 0..<self.columnCount() {
            self.columnNames.append(self.fmResultSet.columnNameForIndex(Int32(index)))
        }
    }
}