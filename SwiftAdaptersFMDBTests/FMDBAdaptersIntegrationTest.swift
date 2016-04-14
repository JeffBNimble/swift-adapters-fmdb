//
//  FMDBAdaptersIntegrationTest.swift
//  SwiftAdaptersFMDBTests
//
//  Created by Jeff Roberts on 7/18/15.
//  Copyright © 2015 nimbleNoggin.io. All rights reserved.
//

import XCTest
@testable import SwiftAdaptersFMDB
import SwiftProtocolsSQLite
import FMDB

class FMDBAdaptersIntegrationTest: XCTestCase {
    var database:SQLiteDatabase!
    var cursor:Cursor?

    override func setUp() {
        super.setUp()

        database = FMDBDatabaseWrapper(path: "")
    }
    
    override func tearDown() {
        super.tearDown()

        do {
            try database.close()
        } catch _ {}

        if let cursor = self.cursor {
            cursor.close()
        }
    }
    
    func test_isOpen_databaseHasNotBeenOpened_answersFalse() {
        XCTAssertFalse(database.isOpen)
    }

    func test_isOpen_databaseHasBeenOpened_answersTrue() {
        do {
            try database.open()
        } catch _ {
            XCTFail("Database could not be opened")
        }

        XCTAssertTrue(database.isOpen)
    }

    func test_open_databaseWasNotAlreadyOpen_answersTrue() {
        var wasOpened = false
        do {
            try wasOpened = database.open()
        } catch _ {
            XCTFail("Database could not be opened")
        }

        XCTAssertTrue(wasOpened)
    }

    func test_open_databaseWasAlreadyOpen_answersTrue() {
        var wasOpened = false
        do {
            try wasOpened = database.open()
            try database.open()
        } catch _ {
            XCTFail("Database could not be opened")
        }

        XCTAssertTrue(wasOpened)
    }

    func test_close_databaseWasNotAlreadyOpen_answersTrue() {
        var wasClosed = false
        do {
            try wasClosed = database.close()
        } catch _ {
            XCTFail("An exception should have been thrown attempting to close a database that was not open")
        }

        XCTAssertTrue(wasClosed)
    }

    func test_close_databaseWasOpen_answersTrue() {
        var wasClosed = false
        do {
            try database.open()
            try wasClosed = database.close()
        } catch {
            XCTFail("An exception should not have been thrown attempting to close a database that was open: \(error)")
        }

        XCTAssertTrue(wasClosed)
    }

    func test_startTransaction_databaseIsOpen_answersTrue() {
        var wasTransactionStarted = false
        do {
            try database.open()
            wasTransactionStarted = try database.startTransaction()
        } catch {
            XCTFail("An exception should not have been thrown attempting to start a transaction: \(error)")
        }

        XCTAssertTrue(wasTransactionStarted)
    }

    func test_startTransaction_databaseIsNotOpen_throwsException() {
        var wasTransactionStarted = false
        do {
            wasTransactionStarted = try database.startTransaction()
            XCTFail("An exception should have been thrown attempting to start a transaction")
        } catch {
            XCTAssertFalse(wasTransactionStarted)
        }
    }

    func test_commit_databaseIsNotOpen_throwsException() {
        var wasTransactionCommitted = false
        do {
            wasTransactionCommitted = try database.commit()
            XCTFail("An exception should have been thrown attempting to commit a transaction")
        } catch {
            XCTAssertFalse(wasTransactionCommitted)
        }
    }

    func test_commit_databaseIsOpenButNoTransactionStarted_throwsException() {
        var wasTransactionCommitted = false
        do {
            try database.open()
            wasTransactionCommitted = try database.commit()
            XCTFail("An exception should have been thrown attempting to commit a transaction")
        } catch {
            XCTAssertFalse(wasTransactionCommitted)
        }
    }

    func test_commit_databaseIsOpenAndTransactionStarted_answersTrue() {
        var wasTransactionCommitted = false
        do {
            try database.open()
            try database.startTransaction()
            wasTransactionCommitted = try database.commit()
        } catch {
            XCTFail("An exception should not have been thrown attempting to commit a transaction")
        }

        XCTAssertTrue(wasTransactionCommitted)
    }

    func test_rollback_databaseIsNotOpen_throwsException() {
        var wasTransactionRolledBack = false
        do {
            wasTransactionRolledBack = try database.rollback()
            XCTFail("An exception should have been thrown attempting to rollback a transaction")
        } catch {
            XCTAssertFalse(wasTransactionRolledBack)
        }
    }

    func test_rollback_databaseIsOpenButNoTransactionStarted_throwsException() {
        var wasTransactionRolledBack = false
        do {
            try database.open()
            wasTransactionRolledBack = try database.rollback()
            XCTFail("An exception should have been thrown attempting to rollback a transaction")
        } catch {
            XCTAssertFalse(wasTransactionRolledBack)
        }
    }

    func test_rollback_databaseIsOpenAndTransactionStarted_answersTrue() {
        var wasTransactionRolledBack = false
        do {
            try database.open()
            try database.startTransaction()
            wasTransactionRolledBack = try database.rollback()
        } catch {
            XCTFail("An exception should not have been thrown attempting to rollback a transaction")
        }

        XCTAssertTrue(wasTransactionRolledBack)
    }

    func test_executeUpdate_validSQLDeleteStatement_answersCorrectRowsDeleted() {
        var rowsDeleted = 0
        let rows = createTables()
        do {
            rowsDeleted = try database.executeUpdate("DELETE FROM MY_TABLE")
        } catch {
            XCTFail("An exception should not have been thrown attempting to delete rows from a table")
        }

        XCTAssertEqual(rowsDeleted, rows)
    }

    func test_executeUpdate_invalidSQLDeleteStatement_throwsException() {
        var rowsDeleted = 0
        createTables()
        do {
            rowsDeleted = try database.executeUpdate("DELETE FROM INVALID_TABLE")
            XCTFail("An exception should have been thrown attempting to delete rows from a table")
        } catch {
            XCTAssertEqual(rowsDeleted, 0)
        }
    }

    func test_executeUpdateWithParameters_validSQLDeleteStatement_answersCorrectRowsDeleted() {
        var rowsDeleted = 0
        createTables()
        do {
            rowsDeleted = try database.executeUpdate("DELETE FROM MY_TABLE WHERE A = ?", parameters: ["First"])
        } catch {
            XCTFail("An exception should not have been thrown attempting to delete rows from a table")
        }

        XCTAssertEqual(rowsDeleted, 1)
    }

    func test_executeUpdateWithParameters_invalidSQLDeleteStatement_throwsException() {
        var rowsDeleted = 0
        createTables()
        do {
            rowsDeleted = try database.executeUpdate("DELETE FROM INVALID_TABLE WHERE A = ?", parameters: ["Second"])
            XCTFail("An exception should have been thrown attempting to delete rows from a table")
        } catch {
            XCTAssertEqual(rowsDeleted, 0)
        }
    }

    func test_executeUpdateWithNamedParameters_validSQLDeleteStatement_answersCorrectRowsDeleted() {
        var rowsDeleted = 0
        createTables()
        do {
            rowsDeleted = try database.executeUpdate("DELETE FROM MY_TABLE WHERE A = :value", parameters: ["value" : "First"])
        } catch {
            XCTFail("An exception should not have been thrown attempting to delete rows from a table")
        }

        XCTAssertEqual(rowsDeleted, 1)
    }

    func test_executeUpdateWithNamedParameters_invalidSQLDeleteStatement_throwsException() {
        var rowsDeleted = 0
        createTables()
        do {
            rowsDeleted = try database.executeUpdate("DELETE FROM INVALID_TABLE WHERE A = :value", parameters: ["value" : "Second"])
            XCTFail("An exception should have been thrown attempting to delete rows from a table")
        } catch {
            XCTAssertEqual(rowsDeleted, 0)
        }
    }

    func test_executeQuery_validSQLSelectStatement_answersACursor() {
        createTables()
        do {
           cursor = try database.executeQuery("SELECT * FROM MY_TABLE")
        } catch {
            XCTFail("An exception should not have been thrown attempting to query rows from a table")
        }

        XCTAssertNotNil(cursor)
    }

    func test_executeQuery_invalidSQLSelectStatement_throwsException() {
        createTables()
        do {
            cursor = try database.executeQuery("SELECT * FROM INVALID_TABLE")
            XCTFail("An exception should have been thrown attempting to query rows from a table")
        } catch {
            XCTAssertNil(cursor)
        }
    }

    func test_executeQueryWithParameters_validSQLSelectStatement_answersACursor() {
        createTables()
        do {
            cursor = try database.executeQuery("SELECT * FROM MY_TABLE WHERE A = ?", parameters: ["Second"])
        } catch {
            XCTFail("An exception should not have been thrown attempting to query rows from a table")
        }

        XCTAssertNotNil(cursor)
    }

    func test_executeQueryWithParameters_invalidSQLSelectStatement_throwsException() {
        createTables()
        do {
            cursor = try database.executeQuery("SELECT * FROM INVALID_TABLE WHERE A = ?", parameters: ["Second"])
            XCTFail("An exception should have been thrown attempting to query rows from a table")
        } catch {
            XCTAssertNil(cursor)
        }
    }

    func test_executeQueryWithNamedParameters_validSQLSelectStatement_answersACursor() {
        createTables()
        do {
            cursor = try database.executeQuery("SELECT * FROM MY_TABLE WHERE A = :value", parameters: ["value" : "First"])
        } catch {
            XCTFail("An exception should not have been thrown attempting to query rows from a table")
        }

        XCTAssertNotNil(cursor)
    }

    func test_executeQueryWithNamedParameters_invalidSQLSelectStatement_throwsException() {
        createTables()
        do {
            cursor = try database.executeQuery("SELECT * FROM INVALID_TABLE WHERE A = :value", parameters: ["value" : "First"])
            XCTFail("An exception should have been thrown attempting to query rows from a table")
        } catch {
            XCTAssertNil(cursor)
        }
    }

    func test_executeQuery_queryIsCountQuery_returnsCursorWithRowCountColumn() {
        createTables()

        do {
            cursor = try database.executeQuery("SELECT COUNT(*) as row_count FROM MY_TABLE")
            cursor!.moveToFirst()
        } catch {
            XCTFail("An unexpected error occurred attempting to query the database: \(error)")
        }

        XCTAssertTrue(cursor!.intFor(0) > 0)
    }
    
//    func testPerformanceExample() {
//        // This is an example of a performance test case.
//        self.measureBlock {
//            // Put the code you want to measure the time of here.
//        }
//    }

    private func createTables() -> Int {
        var rowCount = 0
        do {
            try database.open()

            try database.executeUpdate("CREATE TABLE MY_TABLE (A TEXT, B INTEGER)")

            rowCount = try database.executeUpdate("INSERT INTO MY_TABLE (A, B) VALUES ('First', 1)") + rowCount
            rowCount = try database.executeUpdate("INSERT INTO MY_TABLE (A, B) VALUES ('Second', 2)") + rowCount
            rowCount = try database.executeUpdate("INSERT INTO MY_TABLE (A, B) VALUES ('Third', 3)") + rowCount

        } catch {
            print("An unexpected error occurred attempting to create a database: \(error)")
        }

        return rowCount
    }
    
}
