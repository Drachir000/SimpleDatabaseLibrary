package de.drachir000.util.simpledatabaselibrary;

import de.drachir000.util.simpledatabaselibrary.configuration.DatabaseConfig;
import de.drachir000.util.simpledatabaselibrary.configuration.DatabaseType;
import de.drachir000.util.simpledatabaselibrary.exception.DatabaseException;
import de.drachir000.util.simpledatabaselibrary.exception.QueryException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

@DisplayName("Database Tests")
class DatabaseTest {

    private Database database;
    private String testDbPath;

    @BeforeEach
    void setUp() throws Exception {

        testDbPath = "test_database_" + System.currentTimeMillis() + ".db";
        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.SQLITE)
                .database(testDbPath);

        database = new Database(config);

        database.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)");
        database.execute("INSERT INTO users (name, email, age) VALUES (?, ?, ?)", "Alice", "alice@test.com", 25);
        database.execute("INSERT INTO users (name, email, age) VALUES (?, ?, ?)", "Bob", "bob@test.com", 30);

    }

    @AfterEach
    void tearDown() {

        if (database != null) {
            database.close();
        }

        new File(testDbPath).delete();

    }

    @Test
    @DisplayName("Should execute raw SELECT and DML statements")
    void testExecuteRawSql() throws QueryException {

        QueryResult update = database.execute(
                "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                "Charlie", "charlie@test.com", 35
        );

        assertThat(update.isUpdate()).isTrue();
        assertThat(update.getUpdateCount()).isEqualTo(1);

        QueryResult select = database.execute("SELECT * FROM users WHERE age >= ?", 30);
        assertThat(select.isUpdate()).isFalse();
        assertThat(select.size()).isEqualTo(2);

    }

    @Test
    @DisplayName("Should commit successful transaction")
    void testTransactionCommit() throws Exception {

        String result = database.transaction(() -> {
            database.execute("INSERT INTO users (name, email, age) VALUES (?, ?, ?)", "Dave", "dave@test.com", 28);
            database.execute("UPDATE users SET age = age + 1 WHERE name = ?", "Alice");
            return "OK";
        });

        assertThat(result).isEqualTo("OK");

        QueryResult verifyDave = database.execute("SELECT * FROM users WHERE name = ?", "Dave");
        assertThat(verifyDave.size()).isEqualTo(1);

        QueryResult verifyAlice = database.execute("SELECT age FROM users WHERE name = ?", "Alice");
        assertThat(verifyAlice.getFirst()).isPresent();
        assertThat(verifyAlice.getFirst().orElseThrow().get("age")).isEqualTo(26);

    }

    @Test
    @DisplayName("Should rollback transaction on exception")
    void testTransactionRollbackOnException() {

        assertThatThrownBy(() -> database.transaction(() -> {
            database.execute("INSERT INTO users (name, email, age) VALUES (?, ?, ?)", "Eve", "eve@test.com", 22);
            throw new RuntimeException("fail");
        }))
                .isInstanceOf(DatabaseException.class)
                .hasMessageContaining("Transaction failed");

        // Eve must not be present
        assertThatCode(() -> {
            QueryResult result = database.execute("SELECT * FROM users WHERE name = ?", "Eve");
            assertThat(result.size()).isZero();
        }).doesNotThrowAnyException();

    }

    @Test
    @DisplayName("Should handle nested transactions within a single outer transaction")
    void testNestedTransactions() throws Exception {

        Integer sum = database.transaction(() -> {
            database.execute("INSERT INTO users (name, email, age) VALUES (?, ?, ?)", "Nested", "nested@test.com", 20);
            return database.transaction(() -> {
                database.execute("INSERT INTO users (name, email, age) VALUES (?, ?, ?)", "Inner", "inner@test.com", 21);
                return 40 + 2;
            });
        });

        assertThat(sum).isEqualTo(42);

        QueryResult verify = database.execute("SELECT name FROM users WHERE name IN (?, ?)", "Nested", "Inner");
        assertThat(verify.size()).isEqualTo(2);

    }

    @Test
    @DisplayName("Should support convenience methods: insert, select, update, delete")
    void testConvenienceMethods() throws QueryException {

        QueryResult inserted = database.insert("users", Map.of(
                "name", "Frank",
                "email", "frank@test.com",
                "age", 50
        ));
        assertThat(inserted.isUpdate()).isTrue();
        assertThat(inserted.getUpdateCount()).isEqualTo(1);

        QueryResult selected = database.select("users");
        assertThat(selected.size()).isEqualTo(3);

        QueryResult updated = database.update("users", Map.of("email", "alice+updated@test.com"), "name = ?", "Alice");
        assertThat(updated.getUpdateCount()).isEqualTo(1);

        QueryResult deleted = database.delete("users", "name = ?", "Bob");
        assertThat(deleted.getUpdateCount()).isEqualTo(1);

        QueryResult verify = database.execute("SELECT COUNT(*) as cnt FROM users");
        assertThat(verify.getFirst().orElseThrow().get("cnt")).isEqualTo(2);

    }

    @Test
    @DisplayName("Should throw QueryException for invalid SQL")
    void testExecuteInvalidSql() {
        assertThatThrownBy(() -> database.execute("THIS IS NOT SQL"))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Query execution failed");
    }

    @Test
    @DisplayName("Should fail to acquire connection after close")
    void testCloseThenExecute() {
        database.close();
        assertThatThrownBy(() -> database.execute("SELECT 1"))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Failed to acquire connection");
    }

}
