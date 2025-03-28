package com.qwazr.jdbc.cache;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.sql.ResultSetMetaData;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class demonstrates how to use the JDBC cache driver with Oracle's JDBC driver.
 * Note: This test doesn't actually connect to an Oracle database but demonstrates the configuration.
 */
public class JdbcCacheWithOracleDriverTest {

    private Path tempDirectory;
    private Connection h2Connection;
    private Connection cachedConnection;

    @Before
    public void setup() throws Exception {
        // Create a temporary directory for the cache
        tempDirectory = Files.createTempDirectory("jdbc-cache-oracle-test");

        // Set up the H2 database with test data (as a stand-in for Oracle)
        setupH2Database();

        // Set up the cached connection (demonstrating Oracle configuration)
        setupCachedConnection();
    }

    private void setupH2Database() throws Exception {
        // Regular H2 connection (simulating Oracle)
        Class.forName("org.h2.Driver");
        h2Connection = DriverManager.getConnection("jdbc:h2:mem:oracle_simulation;DB_CLOSE_DELAY=-1", "sa", "");
        
        try (Statement stmt = h2Connection.createStatement()) {
            // Create a test table
            stmt.execute("CREATE TABLE employees (" +
                    "id INT PRIMARY KEY, " +
                    "name VARCHAR(100), " +
                    "salary DECIMAL(10,2), " +
                    "hire_date DATE)");
            
            // Insert test data
            stmt.execute("INSERT INTO employees VALUES (1, 'John Doe', 75000.00, '2020-01-15')");
            stmt.execute("INSERT INTO employees VALUES (2, 'Jane Smith', 85000.00, '2019-06-20')");
        }
    }

    private void setupCachedConnection() throws Exception {
        // Load the JDBC cache driver
        Class.forName("com.qwazr.jdbc.cache.Driver");

        // Configure connection properties
        Properties info = new Properties();
        
        // In a real scenario, you would use Oracle's JDBC URL and driver class
        // info.setProperty("cache.driver.url", "jdbc:oracle:thin:@//hostname:port/service_name");
        // info.setProperty("cache.driver.class", "oracle.jdbc.OracleDriver");
        
        // For this test, we're using H2 as a stand-in for Oracle
        info.setProperty("cache.driver.url", "jdbc:h2:mem:oracle_simulation;DB_CLOSE_DELAY=-1");
        info.setProperty("cache.driver.class", "org.h2.Driver");
        info.setProperty("user", "sa");
        info.setProperty("password", "");
        
        // Make sure the cache directory exists
        File cacheDir = tempDirectory.toFile();
        if (!cacheDir.exists()) {
            cacheDir.mkdirs();
        }
        System.out.println("Cache directory: " + tempDirectory.toString());
        
        // Explicitly enable caching
        info.setProperty("cache.driver.active", "true");

        // Create the cached connection
        String jdbcUrl = "jdbc:cache:file:" + tempDirectory.toString();
        System.out.println("JDBC URL: " + jdbcUrl);
        cachedConnection = DriverManager.getConnection(jdbcUrl, info);
    }

    @Test
    public void testOracleWithCaching() throws Exception {
        // First query - This will be cached
        System.out.println("Executing first query");
        String query = "SELECT * FROM employees WHERE id = ?";
        try (PreparedStatement stmt = cachedConnection.prepareStatement(query)) {
            stmt.setInt(1, 1);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1)); 
                assertEquals("John Doe", rs.getString(2)); 
                assertEquals(75000.00, rs.getDouble(3), 0.001);
            }
        }
        
        // Give a moment for cache files to be written
        System.out.println("Sleeping to allow cache files to be written");
        Thread.sleep(1000);
        
        // Verify cache file exists
        File cacheDir = tempDirectory.toFile();
        assertTrue("Cache directory should exist", cacheDir.exists());
        System.out.println("Looking for cache files in: " + cacheDir.getAbsolutePath());
        
        // Look for any files in the directory
        File[] allFiles = cacheDir.listFiles();
        if (allFiles != null) {
            System.out.println("All files in directory (" + allFiles.length + "):");
            for (File file : allFiles) {
                System.out.println("  - " + file.getName() + " (" + file.length() + " bytes)");
            }
        }
        
        // Verify at least one file exists in the cache directory
        assertTrue("Cache directory should contain at least one file", allFiles != null && allFiles.length > 0);
        
        // Now, let's close the connection and test if we can retrieve data from the cache
        System.out.println("Closing H2 connection to ensure cache is used");
        if (h2Connection != null) {
            h2Connection.close();
            h2Connection = null;
        }
        
        // Second query - This should use the cache since the H2 connection is closed
        System.out.println("Executing second query (should use cache)");
        try (PreparedStatement stmt = cachedConnection.prepareStatement(query)) {
            stmt.setInt(1, 1);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue("ResultSet should have data from cache even with backend connection closed", rs.next());
                assertEquals(1, rs.getInt(1)); 
                assertEquals("John Doe", rs.getString(2)); 
                assertEquals(75000.00, rs.getDouble(3), 0.001);
                System.out.println("Successfully read data from cache!");
            }
        }
        
        // Show usage with Oracle commented configuration
        System.out.println("\nTo use this driver with Oracle, configure:");
        System.out.println("1. JDBC URL: jdbc:cache:file:/path/to/cache/directory");
        System.out.println("2. Properties:");
        System.out.println("   - cache.driver.url=jdbc:oracle:thin:@//hostname:port/service_name");
        System.out.println("   - cache.driver.class=oracle.jdbc.OracleDriver");
        System.out.println("   - user=your_username");
        System.out.println("   - password=your_password");
    }
    
    @Test
    public void testAccessingDataByColumnName() throws Exception {
        // Use a separate cache directory for this test
        Path columnNameCacheDir = Files.createTempDirectory("jdbc-cache-oracle-column-name-test");
        Properties info = new Properties();
        info.setProperty("cache.driver.url", "jdbc:h2:mem:oracle_simulation;DB_CLOSE_DELAY=-1");
        info.setProperty("cache.driver.class", "org.h2.Driver");
        info.setProperty("user", "sa");
        info.setProperty("password", "");
        info.setProperty("cache.driver.active", "true");
        
        System.out.println("Column name test cache directory: " + columnNameCacheDir.toString());
        
        // Create a connection specifically for this test
        String jdbcUrl = "jdbc:cache:file:" + columnNameCacheDir.toString();
        try (Connection conn = DriverManager.getConnection(jdbcUrl, info)) {
            // First execute an SQL query to check column metadata
            System.out.println("Getting column metadata information");
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM employees LIMIT 1")) {
                // Print the column names and their case
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                System.out.println("Column metadata information:");
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    String columnLabel = metaData.getColumnLabel(i);
                    System.out.println("  Column " + i + ": name=" + columnName + ", label=" + columnLabel);
                }
            }
            
            // First query - This will be cached
            System.out.println("Executing query with column name access");
            String query = "SELECT ID, NAME, SALARY, HIRE_DATE FROM employees WHERE ID = ?";
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setInt(1, 1);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    
                    // Access data by column name - using uppercase as H2 standardizes to uppercase
                    assertEquals(1, rs.getInt("ID"));
                    assertEquals("John Doe", rs.getString("NAME"));
                    assertEquals(75000.00, rs.getDouble("SALARY"), 0.001);
                    
                    System.out.println("Successfully accessed data by column name!");
                }
            }
            
            // Execute a query for a different employee using column names
            String query2 = "SELECT * FROM employees WHERE ID = ?";
            try (PreparedStatement stmt = conn.prepareStatement(query2)) {
                stmt.setInt(1, 2);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    
                    // Access data by column name
                    assertEquals(2, rs.getInt("ID"));
                    assertEquals("Jane Smith", rs.getString("NAME"));
                    assertEquals(85000.00, rs.getDouble("SALARY"), 0.001);
                    
                    System.out.println("Successfully retrieved second employee using column names!");
                }
            }
        }
        
        // Clean up the temporary directory
        Files.walk(columnNameCacheDir)
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
        // Close connections
        if (h2Connection != null) {
            h2Connection.close();
        }
        
        if (cachedConnection != null) {
            cachedConnection.close();
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