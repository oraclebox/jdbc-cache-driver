# jdbc-cache-driver

[![Build Status](https://travis-ci.org/qwazr/jdbc-cache-driver.svg?branch=master)](https://travis-ci.org/qwazr/jdbc-cache-driver)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.qwazr/jdbc-cache-driver/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.qwazr/jdbc-cache-driver)
[![Join the chat at https://gitter.im/qwazr/QWAZR](https://badges.gitter.im/qwazr/QWAZR.svg)](https://gitter.im/qwazr/QWAZR)
[![Javadocs](http://www.javadoc.io/badge/com.qwazr/jdbc-cache-driver.svg)](http://www.javadoc.io/doc/com.qwazr/jdbc-cache-driver)
[![Coverage Status](https://coveralls.io/repos/github/qwazr/jdbc-cache-driver/badge.svg?branch=master)](https://coveralls.io/github/qwazr/jdbc-cache-driver?branch=master)

JDBC-Driver-Cache is JDBC cache which store the result of a SQL query (ResultSet) in files or in memory.
The same query requested again will be read from the file, the database is no more requested again.

You may use it to easily mock ResultSets from a database.

JDBC-Driver-Cache is itself a JDBC driver and acts as a wrapper over any third-party JDBC driver.

Usage
-----

### Add the driver in your maven projet

The library is available on Maven Central.

```xml
<dependency>
  <groupId>com.qwazr</groupId>
  <artifactId>jdbc-cache-driver</artifactId>
  <version>1.3</version>
</dependency>
```

### JAVA Code example

First, you have to initialize the JDBC drivers.
In this example we use Apache Derby as backend driver.
You can use any compliant JDBC driver.

```java
// Initialize the cache driver
Class.forName("com.qwazr.jdbc.cache.Driver");

// Provide the URL and the Class name of the backend driver
Properties info = new Properties();
info.setProperty("cache.driver.url", "jdbc:derby:memory:myDB;create=true");
info.setProperty("cache.driver.class", "org.apache.derby.jdbc.EmbeddedDriver");
```

Use the file cache implementation:

```java
// Get your JDBC connection
Connection cnx = DriverManager.getConnection("jdbc:cache:file:/var/jdbc/cache", info);
```

Or use the in memory cache implementation:

```java
// Get your JDBC connection
Connection cnx = DriverManager.getConnection("jdbc:cache:mem:my-memory-cache", info);
```

To build a connection you have to provide the URL and some properties.
The URL tells the driver where to store the cached ResultSet.

The syntax of the URL can be:

* *jdbc:cache:file:{path-to-the-cache-directory}* for on disk cache
* *jdbc:cache:mem:{name-of-the-cache}* for in memory cache

Two possible properties:
- **cache.driver.url** contains the typical JDBC URL of the backend driver.
- **cache.driver.class** contains the class name of the backend driver.

The properties are passed to both the cache driver and the backend driver.

### Use in transparent mode

You can also disable the cache by setting **false** to the property **cache.driver.active**.
In this mode, the cache driver is transparent. All the queries and the result handled by the backend-driver.

```java
info.setProperty("cache.driver.active", "false");
Connection cnx = DriverManager.getConnection("jdbc:cache:file:/var/jdbc/cache", info);
```

### Best Practices for Column Name Handling

When using the JDBC cache driver, it's important to understand how column names are handled:

1. **Case Sensitivity**: The JDBC cache driver requires exact case matching for column names, even when the underlying database supports case-insensitive column access.

2. **Column Name Access**:
   - Always use the exact case of column names as they are stored in the database
   - For H2 databases, column names are typically stored in uppercase by default
   - When using quoted identifiers in H2, the case is preserved as specified

3. **Example with H2**:
```java
// H2 typically stores column names in uppercase
ResultSet rs = stmt.executeQuery("SELECT * FROM employees");
rs.getInt("ID");        // Works
rs.getInt("id");        // May fail
rs.getInt("Id");        // May fail

// With quoted identifiers, case is preserved
ResultSet rs = stmt.executeQuery("SELECT \"userId\", \"firstName\" FROM users");
rs.getInt("userId");    // Works
rs.getInt("USERID");    // May fail
```

4. **Database-Specific Behavior**:
   - Oracle: Column names are typically uppercase unless quoted
   - H2: Column names are uppercase by default unless quoted
   - Other databases may have different default behaviors

5. **Troubleshooting**:
   - If you encounter "Column not found" errors, check the actual column names using `ResultSetMetaData`
   - Print column metadata to verify the exact case of column names:
```java
ResultSetMetaData metaData = rs.getMetaData();
for (int i = 1; i <= metaData.getColumnCount(); i++) {
    System.out.println("Column " + i + ": " + metaData.getColumnName(i));
}
```

Community
---------

JDBC-Driver-Cache is open source and is licensed under the Apache 2.0 License.

Report any issue here:
https://github.com/qwazr/jdbc-cache-driver/issues
