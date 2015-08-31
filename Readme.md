# Introduction
swift-adapters-fmdb is an iOS framework written in Swift 2. It is an a set of classes that conform to the [swift-protocols-sqlite](https://github.com/JeffBNimble/swift-protocols-sqlite) protocols so that you may seamlessly plug-in this implementation to your application that is written against the protocols.

I had to fork fmdb to add shared a shared schema so that I could build fmdb with [Carthage](https://github.com/Carthage/Carthage). If and when fmdb ever supports Carthage builds, the dependency on the fork can be replaced with a direct reference to fmdb.

You can find my forked repo [here](https://github.com/JeffBNimble/fmdb). All changes have been  made in the carthage branch.

# Using the FMDB adapter
As long as your application uses the swift-protocols-sqlite API/framework, you can easily select/use FMDB as your underlying SQLite library by using this framework. The only class you really need in your app is the [FMDBDatabaseFactory](https://github.com/JeffBNimble/swift-adapters-fmdb/blob/master/SwiftAdaptersFMDB/database/util/FMDBAdapters.swift#L12).

**Creating an in-memory SQLite database**

```swift
do {
    let factory = FMDBDatabaseFactory()
    let database : SQLiteDatabase = factory.create(nil)
} catch {
  // Do something with the error
}
```
Note that the return type of the factory.create() function is a protocol; SQLiteDatabase

**Creating a temporary SQLite database**

```swift
do {
    let factory = FMDBDatabaseFactory()
    let database : SQLiteDatabase = factory.create("")
} catch {
  // Do something with the error
}
```

**Creating a SQLite database stored on the filesystem **

```swift
do {
    let factory = FMDBDatabaseFactory()
    let database : SQLiteDatabase = factory.create("inventory.sqlite3")
} catch {
  // Do something with the error
}
```

# Installation
Use [Carthage](https://github.com/Carthage/Carthage). This framework requires the use of Swift 2 and XCode 7 or greater.

Specify the following in your Cartfile to use swift-adapters-fmdb:

```github "JeffBNimble/swift-adapters-fmdb" "0.0.13"```

This library/framework has its own set of dependencies and you should use ```carthage update```. The framework dependencies are specified in the [Cartfile](https://github.com/JeffBNimble/swift-adapters-fmdb/blob/master/Cartfile).
