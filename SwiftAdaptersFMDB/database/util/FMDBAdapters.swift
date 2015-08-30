//
// Created by Jeff Roberts on 7/18/15.
// Copyright (c) 2015 nimbleNoggin.io. All rights reserved.
//

import Foundation
import SwiftProtocolsSQLite
import SwiftProtocolsCore
import fmdbframework

@objc
public class FMDBDatabaseFactory:DatabaseFactory {
    public required init() {}
    
    override public func create(with:String?) throws -> SQLiteDatabase {
        return FMDBDatabaseWrapper(path:with) as SQLiteDatabase
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
    private(set) public var isOpen:Bool = false
    
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
        self.isOpen = self.fmdatabase.open()
        guard self.fmdatabase.hadError() else {
            return self.isOpen
        }
        
        throw self.fmdatabase.lastError()
    }
    
    /// close: Close the database
    /// Returns: A boolean indicating whether or not the database was successfully closed
    /// THrows: A SQLiteDatabaseError if the database could not be opened
    public func close() throws -> Bool {
        let wasClosed = self.fmdatabase.close()
        guard self.fmdatabase.hadError() else {
            self.isOpen = false
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
    /// A flag indicating whether the end of the wrapped FMResultSet has been reached
    private var atEnd = false
    
    /// A cached array of column names in column index order
    private var columnNames = [String]()
    
    // The current 0-based cursor position
    private var cursorPosition = -1
    
    // The wrapped FMDB FMResultSet
    private var fmResultSet:FMResultSet
    
    // An Array of dictionaries (1 per row) containing the rows cached (which are cached upon access)
    private var rowCache:[[String:AnyObject]]
    
    public required init(resultSet:FMResultSet) {
        self.fmResultSet = resultSet
        
        // Initialize properties
        self.rowCache = []
        self.initializedColumnNames()
        
        // Pre-cache some rows
        self.ensureRowCacheUpTo(4) // Cache the first chunk of rows
    }
    
    public func boolFor(columnName:String) -> Bool {
        return self.valueAt(columnName) as! Bool
    }
    
    public func boolFor(columnIndex:Int) -> Bool {
        return self.boolFor(self.columnNameFor(columnIndex))
    }
    
    public func close() -> Void {
        self.columnNames.removeAll()
        self.rowCache.removeAll()
        self.cursorPosition = -1
        self.fmResultSet.close()
    }
    
    public func columnCount() -> Int {
        return self.columnNames.count
    }
    
    public func columnIndexFor(columnName:String) -> Int {
        return self.columnNames.indexOf(columnName.lowercaseString)!
    }
    
    public func columnAtIndexIsNull(columnIndex:Int) -> Bool {
        return self.columnIsNull(self.columnNameFor(columnIndex))
    }
    
    public func columnIsNull(columnName:String) -> Bool {
        guard self.valueAt(columnName) == nil else {
            return true
        }
        
        return false
    }
    
    public func columnNameFor(columnIndex:Int) -> String {
        return self.columnNames[columnIndex]
    }
    
    public func dataFor(columnName:String) -> NSData {
        return self.valueAt(columnName.lowercaseString) as! NSData
    }
    
    public func dataFor(columnIndex:Int) -> NSData {
        return self.dataFor(self.columnNameFor(columnIndex))
    }
    
    public func dateFor(columnName:String) -> NSDate {
        return self.valueAt(columnName) as! NSDate
    }
    
    public func dateFor(columnIndex:Int) -> NSDate {
        return self.dateFor(self.columnNameFor(columnIndex))
    }
    
    public func doubleFor(columnName:String) -> Double {
        return self.valueAt(columnName) as! Double
    }
    
    public func doubleFor(columnIndex:Int) -> Double {
        return self.doubleFor(self.columnNameFor(columnIndex))
    }
    
    public func intFor(columnName:String) -> Int {
        return self.valueAt(columnName) as! Int
    }
    
    public func intFor(columnIndex:Int) -> Int {
        return self.intFor(self.columnNameFor(columnIndex))
    }
    
    public func longFor(columnName:String) -> Int32 {
        return self.valueAt(columnName) as! Int32
    }
    
    public func longFor(columnIndex:Int) -> Int32 {
        return self.longFor(self.columnNameFor(columnIndex))
    }
    
    public func longLongIntFor(columnName:String) -> Int64 {
        return self.valueAt(columnName) as! Int64
    }
    
    public func longLongIntFor(columnIndex:Int) -> Int64 {
        return self.longLongIntFor(self.columnNameFor(columnIndex))
    }
    
    public func move(offset:Int) -> Bool {
        let moved = self.moveToPosition(self.cursorPosition + offset)
        return moved
    }
    
    public func moveToFirst() -> Bool {
        self.ensureRowCacheUpTo(1)
        
        self.cursorPosition = self.rowCache.count == 0 ? self.cursorPosition : 0
        return self.cursorPosition == 0
    }
    
    public func moveToLast() -> Bool {
        self.ensureRowCacheUpTo(Int.max)
        self.cursorPosition = self.rowCache.count - 1
        return self.rowCache.count > 0
    }
    
    public func moveToPosition(absolutePosition:Int) -> Bool {
        // Ensure that the requested position is valid
        guard absolutePosition >= 0 else {
            return false
        }
        
        // Ensure that I've resolved the row cache up to and including the requested position
        self.ensureRowCacheUpTo(absolutePosition)
        self.cursorPosition = absolutePosition < self.rowCache.count ? absolutePosition : self.cursorPosition
        return absolutePosition < self.rowCache.count
    }
    
    public func next() -> Bool {
        return self.move(1)
    }
    
    public func previous() -> Bool {
        guard self.cursorPosition > 0 else {
            return false
        }
        self.cursorPosition--
        return true
    }
    
    public func stringFor(columnName:String) -> String {
        return self.valueAt(columnName) as! String
    }
    
    public func stringFor(columnIndex:Int) -> String {
        return self.stringFor(self.columnNameFor(columnIndex))
    }
    
    public func unsignedLongLongIntFor(columnName:String) -> UInt64 {
        return self.valueAt(columnName) as! UInt64
    }
    
    public func unsignedLongLongIntFor(columnIndex:Int) -> UInt64 {
        return self.unsignedLongLongIntFor(self.columnNameFor(columnIndex))
    }
    
    private func cachedRowAt(cursorPosition:Int) -> [String:AnyObject]? {
        guard cursorPosition <= self.rowCache.count else {
            return nil
        }
        
        return self.rowCache[cursorPosition]
    }
    
    private func ensureRowCacheUpTo(cachePosition:Int) -> Bool {
        let currentMaxPosition = self.rowCache.count - 1
        
        // If we already have more rows in the cache than what is being requested, bail
        guard currentMaxPosition < cachePosition else {
            return true
        }
        
        // If we've already reached the end of the wrapped result set, don't read on
        guard !atEnd else {
            return false
        }
        
        // Fill the rowcache up to the specified position or until we reach the end
        for _ in currentMaxPosition...cachePosition {
            guard self.fmResultSet.next() else {
                self.atEnd = true
                return false
            }
            
            self.rowCache.append(self.fmResultSet.resultDictionary() as! [String:AnyObject])
        }
        
        return true
    }


    private func initializedColumnNames() {
        for index in 0..<self.fmResultSet.columnCount() {
            self.columnNames.append(self.fmResultSet.columnNameForIndex(Int32(index)).lowercaseString)
        }
    }
    
    private func valueAt(columnName:String) -> AnyObject? {
        guard let row = self.cachedRowAt(self.cursorPosition) else {
            return nil
        }
        
        return row[columnName.lowercaseString] is NSNull ? nil : row[columnName.lowercaseString]
    }
}