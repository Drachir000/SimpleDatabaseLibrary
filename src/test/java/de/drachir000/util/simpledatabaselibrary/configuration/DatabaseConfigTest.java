package de.drachir000.util.simpledatabaselibrary.configuration;

import de.drachir000.util.simpledatabaselibrary.exception.ConfigurationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive tests for DatabaseConfig builder.
 * Tests configuration validation, URL building, and all builder methods.
 */
@DisplayName("DatabaseConfig Tests")
class DatabaseConfigTest {

    @Test
    @DisplayName("Should create default configuration")
    void testDefaultConfiguration() {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.SQLITE)
                .database("test.db");

        assertThat(config.getType()).isEqualTo(DatabaseType.SQLITE);
        assertThat(config.getDatabase()).isEqualTo("test.db");
        assertThat(config.getPoolSize()).isEqualTo(5);
        assertThat(config.getConnectionTimeout()).isEqualTo(30000);
        assertThat(config.isAutoCommit()).isTrue();
        assertThat(config.getProperties()).isEmpty();

    }

    @Test
    @DisplayName("Should build complete MySQL configuration")
    void testMySQLConfiguration() {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.MYSQL)
                .host("localhost")
                .port(3306)
                .database("testdb")
                .username("user")
                .password("pass")
                .poolSize(10)
                .connectionTimeout(5000)
                .autoCommit(false)
                .property("useSSL", "false")
                .property("serverTimezone", "UTC");

        assertThat(config.getType()).isEqualTo(DatabaseType.MYSQL);
        assertThat(config.getHost()).isEqualTo("localhost");
        assertThat(config.getPort()).isEqualTo(3306);
        assertThat(config.getDatabase()).isEqualTo("testdb");
        assertThat(config.getUsername()).isEqualTo("user");
        assertThat(config.getPassword()).isEqualTo("pass");
        assertThat(config.getPoolSize()).isEqualTo(10);
        assertThat(config.getConnectionTimeout()).isEqualTo(5000);
        assertThat(config.isAutoCommit()).isFalse();
        assertThat(config.getProperties())
                .hasSize(2)
                .containsEntry("useSSL", "false")
                .containsEntry("serverTimezone", "UTC");

    }

    @Test
    @DisplayName("Should build PostgreSQL configuration")
    void testPostgreSQLConfiguration() {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.POSTGRESQL)
                .host("db.example.com")
                .port(5432)
                .database("mydb")
                .username("postgres")
                .password("secret");

        assertThat(config.getType()).isEqualTo(DatabaseType.POSTGRESQL);
        assertThat(config.getHost()).isEqualTo("db.example.com");
        assertThat(config.getPort()).isEqualTo(5432);

    }

    @Test
    @DisplayName("Should support method chaining")
    void testMethodChaining() {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.SQLITE)
                .database("test.db")
                .poolSize(3)
                .connectionTimeout(1000)
                .autoCommit(true)
                .property("key1", "value1")
                .property("key2", "value2");

        assertThat(config).isNotNull();

    }

    @Test
    @DisplayName("Should build SQLite URL correctly")
    void testSQLiteUrlBuilding() throws ConfigurationException, ConfigurationException {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.SQLITE)
                .database("test.db");

        String url = config.buildUrl();
        assertThat(url).isEqualTo("jdbc:sqlite:test.db");

    }

    @Test
    @DisplayName("Should build SQLite URL with path")
    void testSQLiteUrlWithPath() throws ConfigurationException {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.SQLITE)
                .database("/path/to/database.db");

        String url = config.buildUrl();
        assertThat(url).isEqualTo("jdbc:sqlite:/path/to/database.db");

    }

    @Test
    @DisplayName("Should build MySQL URL with all parameters")
    void testMySQLUrlBuilding() throws ConfigurationException {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.MYSQL)
                .host("localhost")
                .port(3306)
                .database("testdb");

        String url = config.buildUrl();
        assertThat(url).isEqualTo("jdbc:mysql://localhost:3306/testdb");

    }

    @Test
    @DisplayName("Should build MySQL URL with default port")
    void testMySQLUrlWithDefaultPort() throws ConfigurationException {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.MYSQL)
                .host("localhost")
                .database("testdb");

        String url = config.buildUrl();
        assertThat(url).isEqualTo("jdbc:mysql://localhost:3306/testdb");

    }

    @Test
    @DisplayName("Should build MySQL URL with default host")
    void testMySQLUrlWithDefaultHost() throws ConfigurationException {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.MYSQL)
                .database("testdb");

        String url = config.buildUrl();
        assertThat(url).isEqualTo("jdbc:mysql://localhost:3306/testdb");

    }

    @Test
    @DisplayName("Should build PostgreSQL URL correctly")
    void testPostgreSQLUrlBuilding() throws ConfigurationException {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.POSTGRESQL)
                .host("db.server.com")
                .port(5432)
                .database("mydb");

        String url = config.buildUrl();
        assertThat(url).isEqualTo("jdbc:postgresql://db.server.com:5432/mydb");

    }

    @Test
    @DisplayName("Should build PostgreSQL URL with defaults")
    void testPostgreSQLUrlWithDefaults() throws ConfigurationException {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.POSTGRESQL)
                .database("mydb");

        String url = config.buildUrl();
        assertThat(url).isEqualTo("jdbc:postgresql://localhost:5432/mydb");

    }

    @Test
    @DisplayName("Should throw exception when type is missing")
    void testMissingType() {

        DatabaseConfig config = DatabaseConfig.builder()
                .database("test.db");

        assertThatThrownBy(() -> config.buildUrl())
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Database type must be specified");

    }

    @Test
    @DisplayName("Should throw exception when database name is missing")
    void testMissingDatabase() {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.SQLITE);

        assertThatThrownBy(() -> config.buildUrl())
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Database name/path must be specified");

    }

    @Test
    @DisplayName("Should throw exception when database name is blank")
    void testBlankDatabase() {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.SQLITE)
                .database("   ");

        assertThatThrownBy(() -> config.buildUrl())
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Database name/path must be specified");

    }

    @Test
    @DisplayName("Should handle null username and password")
    void testNullCredentials() {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.SQLITE)
                .database("test.db")
                .username(null)
                .password(null);

        assertThat(config.getUsername()).isNull();
        assertThat(config.getPassword()).isNull();

    }

    @Test
    @DisplayName("Should handle custom pool sizes")
    void testCustomPoolSize() {

        DatabaseConfig config1 = DatabaseConfig.builder()
                .type(DatabaseType.SQLITE)
                .database("test.db")
                .poolSize(1);

        DatabaseConfig config2 = DatabaseConfig.builder()
                .type(DatabaseType.SQLITE)
                .database("test.db")
                .poolSize(100);

        assertThat(config1.getPoolSize()).isEqualTo(1);
        assertThat(config2.getPoolSize()).isEqualTo(100);

    }

    @Test
    @DisplayName("Should handle custom timeout values")
    void testCustomTimeout() {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.SQLITE)
                .database("test.db")
                .connectionTimeout(1000);

        assertThat(config.getConnectionTimeout()).isEqualTo(1000);

    }

    @Test
    @DisplayName("Should handle multiple custom properties")
    void testMultipleProperties() {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.MYSQL)
                .database("test")
                .property("prop1", "value1")
                .property("prop2", "value2")
                .property("prop3", "value3");

        assertThat(config.getProperties())
                .hasSize(3)
                .containsKeys("prop1", "prop2", "prop3");

    }

    @Test
    @DisplayName("Should return defensive copy of properties")
    void testPropertiesDefensiveCopy() {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.SQLITE)
                .database("test.db")
                .property("key", "value");

        var props = config.getProperties();
        props.put("newKey", "newValue");

        assertThat(config.getProperties()).hasSize(1);

    }

    @ParameterizedTest
    @EnumSource(DatabaseType.class)
    @DisplayName("Should work with all database types")
    void testAllDatabaseTypes(DatabaseType type) {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(type)
                .database("testdb");

        assertThat(config.getType()).isEqualTo(type);
        assertThatCode(() -> config.buildUrl()).doesNotThrowAnyException();

    }

    @Test
    @DisplayName("Should handle in-memory SQLite")
    void testInMemorySQLite() throws ConfigurationException {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.SQLITE)
                .database(":memory:");

        String url = config.buildUrl();
        assertThat(url).isEqualTo("jdbc:sqlite::memory:");

    }

    @Test
    @DisplayName("Should override default values")
    void testOverrideDefaults() {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.SQLITE)
                .database("test.db")
                .poolSize(20)
                .connectionTimeout(60000)
                .autoCommit(false);

        assertThat(config.getPoolSize()).isEqualTo(20);
        assertThat(config.getConnectionTimeout()).isEqualTo(60000);
        assertThat(config.isAutoCommit()).isFalse();

    }

    @Test
    @DisplayName("Should handle special characters in database name")
    void testSpecialCharactersInDatabaseName() throws ConfigurationException {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.SQLITE)
                .database("test-db_2024.db");

        String url = config.buildUrl();
        assertThat(url).contains("test-db_2024.db");

    }

    @Test
    @DisplayName("Should handle IPv4 host addresses")
    void testIPv4Host() throws ConfigurationException {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.MYSQL)
                .host("192.168.1.100")
                .database("testdb");

        String url = config.buildUrl();
        assertThat(url).contains("192.168.1.100");

    }

    @Test
    @DisplayName("Should handle IPv6 host addresses")
    void testIPv6Host() throws ConfigurationException {

        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.POSTGRESQL)
                .host("::1")
                .database("testdb");

        String url = config.buildUrl();
        assertThat(url).contains("::1");

    }

    @Test
    @DisplayName("Should handle non-standard ports")
    void testNonStandardPorts() throws ConfigurationException {

        DatabaseConfig mysqlConfig = DatabaseConfig.builder()
                .type(DatabaseType.MYSQL)
                .port(3307)
                .database("test");

        DatabaseConfig pgConfig = DatabaseConfig.builder()
                .type(DatabaseType.POSTGRESQL)
                .port(5433)
                .database("test");

        assertThat(mysqlConfig.buildUrl()).contains(":3307/");
        assertThat(pgConfig.buildUrl()).contains(":5433/");

    }

}