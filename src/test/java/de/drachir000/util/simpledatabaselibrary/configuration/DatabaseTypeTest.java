package de.drachir000.util.simpledatabaselibrary.configuration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for DatabaseType enum.
 * Verifies correct driver classes and URL prefixes.
 */
@DisplayName("DatabaseType Tests")
class DatabaseTypeTest {

    @Test
    @DisplayName("Should have correct number of database types")
    void testDatabaseTypeCount() {
        assertThat(DatabaseType.values()).hasSize(3);
    }

    @ParameterizedTest
    @CsvSource({
            "SQLITE, org.sqlite.JDBC, jdbc:sqlite:",
            "MYSQL, com.mysql.cj.jdbc.Driver, jdbc:mysql://",
            "POSTGRESQL, org.postgresql.Driver, jdbc:postgresql://"
    })
    @DisplayName("Should have correct driver class and URL prefix")
    void testDatabaseTypeProperties(String typeName, String expectedDriver, String expectedPrefix) {

        DatabaseType type = DatabaseType.valueOf(typeName);

        assertThat(type.getDriverClass()).isEqualTo(expectedDriver);
        assertThat(type.getUrlPrefix()).isEqualTo(expectedPrefix);

    }

    @Test
    @DisplayName("Should get SQLite properties")
    void testSQLiteProperties() {
        assertThat(DatabaseType.SQLITE.getDriverClass()).isEqualTo("org.sqlite.JDBC");
        assertThat(DatabaseType.SQLITE.getUrlPrefix()).isEqualTo("jdbc:sqlite:");
    }

    @Test
    @DisplayName("Should get MySQL properties")
    void testMySQLProperties() {
        assertThat(DatabaseType.MYSQL.getDriverClass()).isEqualTo("com.mysql.cj.jdbc.Driver");
        assertThat(DatabaseType.MYSQL.getUrlPrefix()).isEqualTo("jdbc:mysql://");
    }

    @Test
    @DisplayName("Should get PostgreSQL properties")
    void testPostgreSQLProperties() {
        assertThat(DatabaseType.POSTGRESQL.getDriverClass()).isEqualTo("org.postgresql.Driver");
        assertThat(DatabaseType.POSTGRESQL.getUrlPrefix()).isEqualTo("jdbc:postgresql://");
    }

    @Test
    @DisplayName("Should parse string to DatabaseType")
    void testValueOf() {
        assertThat(DatabaseType.valueOf("SQLITE")).isEqualTo(DatabaseType.SQLITE);
        assertThat(DatabaseType.valueOf("MYSQL")).isEqualTo(DatabaseType.MYSQL);
        assertThat(DatabaseType.valueOf("POSTGRESQL")).isEqualTo(DatabaseType.POSTGRESQL);
    }

    @Test
    @DisplayName("Should throw exception for invalid type name")
    void testInvalidValueOf() {
        assertThatThrownBy(() -> DatabaseType.valueOf("INVALID"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Should support enum comparison")
    void testEnumComparison() {

        DatabaseType type1 = DatabaseType.SQLITE;
        DatabaseType type2 = DatabaseType.SQLITE;
        DatabaseType type3 = DatabaseType.MYSQL;

        assertThat(type1).isEqualTo(type2);
        assertThat(type1).isNotEqualTo(type3);

    }

    @Test
    @DisplayName("Should convert to string")
    void testToString() {
        assertThat(DatabaseType.SQLITE.toString()).isEqualTo("SQLITE");
        assertThat(DatabaseType.MYSQL.toString()).isEqualTo("MYSQL");
        assertThat(DatabaseType.POSTGRESQL.toString()).isEqualTo("POSTGRESQL");
    }
}