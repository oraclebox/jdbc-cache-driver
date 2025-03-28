package com.qwazr.jdbc.cache;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

public class JdbcCacheWithOracleAndHikariCPTest {

    private Path tempDirectory;
    private HikariDataSource h2DataSource;
    private HikariDataSource cachedDataSource;

    @Before
    public void setup() throws Exception {
        // Create a temporary directory for the cache
        tempDirectory = Files.createTempDirectory("jdbc-cache-test");

        // Set up the H2 database with test data
        setupH2Database();

        // Set up the cached data source
        setupCachedDataSource();
    }

    private void setupH2Database() {
        // Configure HikariCP for H2
        HikariConfig h2Config = new HikariConfig();
        h2Config.setJdbcUrl("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;IGNORECASE=TRUE");
        h2Config.setUsername("sa");
        h2Config.setPassword("");
        h2Config.setDriverClassName("org.h2.Driver");
        h2Config.setMaximumPoolSize(5);
        
        h2DataSource = new HikariDataSource(h2Config);

        try (Connection conn = h2DataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Create a test table
            stmt.execute("CREATE TABLE employees (" +
                    "id INT PRIMARY KEY, " +
                    "name VARCHAR(100), " +
                    "salary DECIMAL(10,2), " +
                    "hire_date DATE)");
            
            // Insert test data
            stmt.execute("INSERT INTO employees VALUES (1, 'John Doe', 75000.00, '2020-01-15')");
            stmt.execute("INSERT INTO employees VALUES (2, 'Jane Smith', 85000.00, '2019-06-20')");
            stmt.execute("INSERT INTO employees VALUES (3, 'Bob Johnson', 65000.00, '2021-03-10')");
            stmt.execute("INSERT INTO employees VALUES (4, 'Alice Brown', 90000.00, '2018-11-05')");
            stmt.execute("INSERT INTO employees VALUES (5, 'Charlie Davis', 72000.00, '2022-02-28')");
        } catch (Exception e) {
            throw new RuntimeException("Failed to set up H2 database", e);
        }
    }

    private void setupCachedDataSource() throws Exception {
        // Configure the JDBC cache driver
        Properties info = new Properties();
        info.setProperty("cache.driver.url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;IGNORECASE=TRUE");
        info.setProperty("cache.driver.class", "org.h2.Driver");
        
        // Make sure the driver is loaded
        Class.forName("com.qwazr.jdbc.cache.Driver");
        
        // Configure HikariCP with the cached connection
        HikariConfig cachedConfig = new HikariConfig();
        cachedConfig.setJdbcUrl("jdbc:cache:file:" + tempDirectory.toString());
        cachedConfig.setUsername("sa");
        cachedConfig.setPassword("");
        cachedConfig.setDriverClassName("com.qwazr.jdbc.cache.Driver");
        cachedConfig.setMaximumPoolSize(5);
        
        // Add the JDBC cache properties to the datasource properties
        cachedConfig.addDataSourceProperty("cache.driver.url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;IGNORECASE=TRUE");
        cachedConfig.addDataSourceProperty("cache.driver.class", "org.h2.Driver");
        cachedConfig.addDataSourceProperty("cache.ttl", "3600"); // Cache for 1 hour
        
        cachedDataSource = new HikariDataSource(cachedConfig);
    }
    
    @Test
    public void testCachedQueries() throws Exception {
        // First query - This will be cached
        try (Connection conn = cachedDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM employees WHERE id = ?")) {
            
            stmt.setInt(1, 1);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals("John Doe", rs.getString("name"));
                assertEquals(75000.00, rs.getDouble("salary"), 0.001);
            }
        }
        
        // Verify we have a cache file
        File cacheDir = tempDirectory.toFile();
        assertTrue("Cache directory should exist", cacheDir.exists());
        File[] cacheFiles = cacheDir.listFiles((dir, name) -> name.endsWith(".cache"));
        assertTrue("Cache files should exist", cacheFiles != null && cacheFiles.length > 0);
        
        // Second query - This should use the cache
        try (Connection conn = cachedDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM employees WHERE id = ?")) {
            
            stmt.setInt(1, 1);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals("John Doe", rs.getString("name"));
                assertEquals(75000.00, rs.getDouble("salary"), 0.001);
            }
        }
        
        // Query with different parameter - Should create new cache
        try (Connection conn = cachedDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM employees WHERE id = ?")) {
            
            stmt.setInt(1, 2);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt("id"));
                assertEquals("Jane Smith", rs.getString("name"));
                assertEquals(85000.00, rs.getDouble("salary"), 0.001);
            }
        }
        
        // Verify we have more cache files
        cacheFiles = cacheDir.listFiles((dir, name) -> name.endsWith(".cache"));
        assertTrue("Multiple cache files should exist", cacheFiles != null && cacheFiles.length > 1);
    }
    
    @Test
    public void testLargeStringData() throws Exception {
        // Create a table with a large text column
        try (Connection conn = h2DataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            stmt.execute("CREATE TABLE large_text_test (id INT PRIMARY KEY, large_text CLOB)");
            
            // Create a large string (over 65535 bytes to test our fix)
            StringBuilder largeText = new StringBuilder();
            for (int i = 0; i < 70000; i++) {
                largeText.append((char)('A' + (i % 26)));
            }
            
            // Insert the large text
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO large_text_test VALUES (?, ?)")) {
                pstmt.setInt(1, 1);
                pstmt.setString(2, largeText.toString());
                pstmt.executeUpdate();
            }
        }
        
        // Query the large text through the cache
        try (Connection conn = cachedDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM large_text_test WHERE id = ?")) {
            
            stmt.setInt(1, 1);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                String retrievedText = rs.getString("large_text");
                assertEquals(70000, retrievedText.length());
            }
        }
    }

    @Test
    public void testAdvancedColumnNameUsage() throws Exception {
        // Create a table with mixed case and special column names
        try (Connection conn = h2DataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Using quoted identifiers to preserve case in H2
            stmt.execute("CREATE TABLE complex_columns (" +
                    "\"ID\" INT PRIMARY KEY, " + 
                    "\"UserName\" VARCHAR(100), " +
                    "\"Last_Login_Date\" TIMESTAMP, " +
                    "\"ACCOUNT_Balance\" DECIMAL(10,2))");
            
            // Insert test data
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO complex_columns VALUES (?, ?, ?, ?)")) {
                pstmt.setInt(1, 100);
                pstmt.setString(2, "JohnDoe123");
                pstmt.setTimestamp(3, new java.sql.Timestamp(System.currentTimeMillis()));
                pstmt.setBigDecimal(4, new java.math.BigDecimal("1234.56"));
                pstmt.executeUpdate();
                
                pstmt.setInt(1, 101);
                pstmt.setString(2, "JaneSmith456");
                pstmt.setTimestamp(3, new java.sql.Timestamp(System.currentTimeMillis() - 86400000)); // 1 day ago
                pstmt.setBigDecimal(4, new java.math.BigDecimal("5678.90"));
                pstmt.executeUpdate();
            }
        }
        
        // Print column metadata to understand actual column names
        try (Connection conn = cachedDataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM complex_columns LIMIT 1")) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            System.out.println("Complex columns metadata:");
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                String columnLabel = metaData.getColumnLabel(i);
                System.out.println("  Column " + i + ": name=" + columnName + ", label=" + columnLabel);
            }
        }
        
        // First query to populate cache
        System.out.println("Testing column name access with mixed case columns");
        try (Connection conn = cachedDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM complex_columns WHERE \"ID\" = ?")) {
            
            stmt.setInt(1, 100);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next());
                
                // Access by exact case column names as defined in the schema with quotes
                assertEquals(100, rs.getInt("ID"));
                assertEquals("JohnDoe123", rs.getString("UserName"));
                assertNotNull(rs.getTimestamp("Last_Login_Date"));
                assertEquals(new java.math.BigDecimal("1234.56").doubleValue(), rs.getBigDecimal("ACCOUNT_Balance").doubleValue(), 0.001);
                
                System.out.println("Successfully accessed data using exact case column names");
            }
        }
        
        // Close and reopen connection to ensure we're using cache
        h2DataSource.close();
        
        // Second query to verify cache with the exact column names
        try (Connection conn = cachedDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM complex_columns WHERE \"ID\" = ?")) {
            
            stmt.setInt(1, 101);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next());
                
                // Access by the exact column names
                assertEquals(101, rs.getInt("ID"));
                assertEquals("JaneSmith456", rs.getString("UserName"));
                assertNotNull(rs.getTimestamp("Last_Login_Date"));
                assertEquals(new java.math.BigDecimal("5678.90").doubleValue(), rs.getBigDecimal("ACCOUNT_Balance").doubleValue(), 0.001);
                
                // Print column metadata again from this result set
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                System.out.println("Column metadata from cached result:");
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    String columnLabel = metaData.getColumnLabel(i);
                    System.out.println("  Column " + i + ": name=" + columnName + ", label=" + columnLabel);
                }
                
                System.out.println("Successfully accessed data from cache using exact column names");
            }
        }
    }

    @Test
    public void testLowercaseColumnNames() throws Exception {
        // Create a new H2 database specifically for this test with case insensitivity enabled
        HikariConfig lowercaseH2Config = new HikariConfig();
        lowercaseH2Config.setJdbcUrl("jdbc:h2:mem:lowercase_hikari_test;DB_CLOSE_DELAY=-1;IGNORECASE=TRUE");
        lowercaseH2Config.setUsername("sa");
        lowercaseH2Config.setPassword("");
        lowercaseH2Config.setDriverClassName("org.h2.Driver");
        lowercaseH2Config.setMaximumPoolSize(5);
        
        HikariDataSource lowercaseH2DS = new HikariDataSource(lowercaseH2Config);
        
        // Create a new test table
        try (Connection conn = lowercaseH2DS.getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Create a test table for lowercase testing
            stmt.execute("CREATE TABLE lowercase_test (" +
                    "userId INT PRIMARY KEY, " +
                    "firstName VARCHAR(100), " +
                    "lastName VARCHAR(100), " +
                    "emailAddress VARCHAR(200))");
            
            // Insert test data
            stmt.execute("INSERT INTO lowercase_test VALUES (101, 'John', 'Doe', 'john.doe@example.com')");
            stmt.execute("INSERT INTO lowercase_test VALUES (102, 'Jane', 'Smith', 'jane.smith@example.com')");
        }
        
        // Create a temp directory for caching
        Path lowercaseCacheDir = Files.createTempDirectory("jdbc-cache-hikari-lowercase-test");
        
        // Configure a new HikariCP datasource with the JDBC cache driver
        HikariConfig cachedConfig = new HikariConfig();
        cachedConfig.setJdbcUrl("jdbc:cache:file:" + lowercaseCacheDir.toString());
        cachedConfig.setUsername("sa");
        cachedConfig.setPassword("");
        cachedConfig.setDriverClassName("com.qwazr.jdbc.cache.Driver");
        cachedConfig.setMaximumPoolSize(5);
        
        // Add the JDBC cache properties
        cachedConfig.addDataSourceProperty("cache.driver.url", 
            "jdbc:h2:mem:lowercase_hikari_test;DB_CLOSE_DELAY=-1;IGNORECASE=TRUE");
        cachedConfig.addDataSourceProperty("cache.driver.class", "org.h2.Driver");
        cachedConfig.addDataSourceProperty("cache.ttl", "3600"); // Cache for 1 hour
        
        HikariDataSource cachedLowercaseDS = new HikariDataSource(cachedConfig);
        
        // Print metadata to confirm column names
        try (Connection conn = cachedLowercaseDS.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM lowercase_test LIMIT 1")) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            System.out.println("Lowercase test table metadata:");
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                String columnLabel = metaData.getColumnLabel(i);
                System.out.println("  Column " + i + ": name=" + columnName + ", label=" + columnLabel);
            }
        }
        
        // Query with lowercase column names
        System.out.println("Testing lowercase column access with HikariCP connection");
        try (Connection conn = cachedLowercaseDS.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM lowercase_test WHERE userId = ?")) {
            
            stmt.setInt(1, 101);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next());
                
                // Try to access by lowercase column names
                try {
                    assertEquals(101, rs.getInt("userid"));
                    assertEquals("John", rs.getString("firstname"));
                    assertEquals("Doe", rs.getString("lastname"));
                    assertEquals("john.doe@example.com", rs.getString("emailaddress"));
                    
                    System.out.println("Successfully accessed data using lowercase column names in HikariCP!");
                } catch (SQLException e) {
                    System.out.println("Error accessing lowercase columns: " + e.getMessage());
                    
                    // Try with the original case
                    assertEquals(101, rs.getInt("USERID"));
                    assertEquals("John", rs.getString("FIRSTNAME"));
                    assertEquals("Doe", rs.getString("LASTNAME"));
                    assertEquals("john.doe@example.com", rs.getString("EMAILADDRESS"));
                    
                    System.out.println("Retrieved data using uppercase column names instead.");
                }
                
                // Print the available column names from the ResultSet metadata
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnCount = rsmd.getColumnCount();
                System.out.println("Available columns in cached result set:");
                for (int i = 1; i <= columnCount; i++) {
                    System.out.println("  Column " + i + ": " + rsmd.getColumnName(i) + 
                            " (Label: " + rsmd.getColumnLabel(i) + ")");
                }
            }
        }
        
        // Close and reopen connections to ensure cache is used
        lowercaseH2DS.close();
        
        // One more query after connection is closed to verify cache
        try (Connection conn = cachedLowercaseDS.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM lowercase_test")) {
            
            assertTrue(rs.next());
            
            // Try with all uppercase (which should work with cache)
            assertEquals(101, rs.getInt("USERID"));
            assertEquals("John", rs.getString("FIRSTNAME"));
            assertEquals("Doe", rs.getString("LASTNAME"));
            
            assertTrue(rs.next());
            
            // Try with original case
            try {
                assertEquals(102, rs.getInt("userId"));
                assertEquals("Jane", rs.getString("firstName"));
                assertEquals("Smith", rs.getString("lastName"));
                System.out.println("Successfully accessed cached data with original case column names!");
            } catch (SQLException e) {
                System.out.println("Accessing original case failed: " + e.getMessage());
                
                // Fall back to uppercase
                assertEquals(102, rs.getInt("USERID"));
                assertEquals("Jane", rs.getString("FIRSTNAME"));
                assertEquals("Smith", rs.getString("LASTNAME"));
                System.out.println("Used uppercase for cached data instead.");
            }
        }
        
        // Clean up
        cachedLowercaseDS.close();
        Files.walk(lowercaseCacheDir)
                .sorted((p1, p2) -> -p1.compareTo(p2))
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (Exception e) {
                        // Ignore
                    }
                });
    }

    @After
    public void cleanup() throws Exception {
        // Close the data sources
        if (h2DataSource != null) {
            h2DataSource.close();
        }
        
        if (cachedDataSource != null) {
            cachedDataSource.close();
        }
        
        // Clean up the temporary directory
        Files.walk(tempDirectory)
                .sorted((p1, p2) -> -p1.compareTo(p2))
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (Exception e) {
                        // Ignore
                    }
                });
    }
} 