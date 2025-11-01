package de.drachir000.util.simpledatabaselibrary;

import de.drachir000.util.simpledatabaselibrary.configuration.DatabaseConfig;
import de.drachir000.util.simpledatabaselibrary.configuration.DatabaseType;
import de.drachir000.util.simpledatabaselibrary.exception.QueryException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive tests for QueryBuilder fluent API.
 * Tests query building, parameter binding, and SQL generation.
 */
@DisplayName("QueryBuilder Tests")
class QueryBuilderTest {

    private Database database;
    private String testDbPath;

    @BeforeEach
    void setUp() throws Exception {

        testDbPath = "test_querybuilder_" + System.currentTimeMillis() + ".db";
        DatabaseConfig config = DatabaseConfig.builder()
                .type(DatabaseType.SQLITE)
                .database(testDbPath);

        database = new Database(config);

        database.execute(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)"
        );
        database.execute(
                "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                "Alice", "alice@test.com", 25
        );
        database.execute(
                "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                "Bob", "bob@test.com", 30
        );
        database.execute(
                "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                "Charlie", "charlie@test.com", 35
        );

    }

    @AfterEach
    void tearDown() {
        if (database != null) {
            database.close();
        }
        new File(testDbPath).delete();
    }

    @Test
    @DisplayName("Should build simple SELECT query")
    void testSimpleSelect() throws QueryException {

        QueryResult result = database.query()
                .select("users")
                .execute();

        assertThat(result.size()).isEqualTo(3);
        assertThat(result.isEmpty()).isFalse();

    }

    @Test
    @DisplayName("Should build SELECT with specific columns")
    void testSelectSpecificColumns() throws QueryException {

        QueryResult result = database.query()
                .select("users", "name", "email")
                .execute();

        assertThat(result.size()).isEqualTo(3);
        Map<String, Object> row = result.getFirst().orElseThrow();
        assertThat(row).containsKeys("name", "email");

    }

    @Test
    @DisplayName("Should build SELECT with WHERE clause")
    void testSelectWithWhere() throws QueryException {

        QueryResult result = database.query()
                .select("users")
                .where("age > ?")
                .param(25)
                .execute();

        assertThat(result.size()).isEqualTo(2);

    }

    @Test
    @DisplayName("Should build SELECT with multiple parameters")
    void testSelectWithMultipleParams() throws QueryException {

        QueryResult result = database.query()
                .select("users")
                .where("age BETWEEN ? AND ?")
                .params(25, 35)
                .execute();

        assertThat(result.size()).isEqualTo(3);

    }

    @Test
    @DisplayName("Should build SELECT with ORDER BY")
    void testSelectWithOrderBy() throws QueryException {

        QueryResult result = database.query()
                .select("users")
                .orderBy("age DESC")
                .execute();

        assertThat(result.size()).isEqualTo(3);
        assertThat(result.getColumn("age")).containsExactly(35, 30, 25);

    }

    @Test
    @DisplayName("Should build SELECT with LIMIT")
    void testSelectWithLimit() throws QueryException {

        QueryResult result = database.query()
                .select("users")
                .limit(2)
                .execute();

        assertThat(result.size()).isEqualTo(2);

    }

    @Test
    @DisplayName("Should build complex SELECT query")
    void testComplexSelect() throws QueryException {

        QueryResult result = database.query()
                .select("users", "name", "age")
                .where("age >= ?")
                .param(30)
                .orderBy("age", "name")
                .limit(5)
                .execute();

        assertThat(result.size()).isEqualTo(2);
        List<Object> names = result.getColumn("name");
        assertThat(names).containsExactly("Bob", "Charlie");

    }

    @Test
    @DisplayName("Should build INSERT query from map")
    void testInsert() throws QueryException {

        Map<String, Object> data = Map.of(
                "name", "David",
                "email", "david@test.com",
                "age", 28
        );

        QueryResult result = database.query()
                .insert("users", data)
                .execute();

        assertThat(result.isUpdate()).isTrue();
        assertThat(result.getUpdateCount()).isEqualTo(1);

        QueryResult verify = database.query()
                .select("users")
                .where("name = ?")
                .param("David")
                .execute();

        assertThat(verify.size()).isEqualTo(1);

    }

    @Test
    @DisplayName("Should build UPDATE query")
    void testUpdate() throws QueryException {

        Map<String, Object> data = Map.of("email", "alice.new@test.com");

        QueryResult result = database.query()
                .update("users", data)
                .where("name = ?")
                .param("Alice")
                .execute();

        assertThat(result.isUpdate()).isTrue();
        assertThat(result.getUpdateCount()).isEqualTo(1);

        QueryResult verify = database.query()
                .select("users")
                .where("name = ?")
                .param("Alice")
                .execute();

        Map<String, Object> row = verify.getFirst().orElseThrow();
        assertThat(row.get("email")).isEqualTo("alice.new@test.com");

    }

    @Test
    @DisplayName("Should build DELETE query")
    void testDelete() throws QueryException {

        QueryResult result = database.query()
                .delete("users")
                .where("name = ?")
                .param("Bob")
                .execute();

        assertThat(result.isUpdate()).isTrue();
        assertThat(result.getUpdateCount()).isEqualTo(1);

        QueryResult verify = database.query().select("users").execute();
        assertThat(verify.size()).isEqualTo(2);

    }

    @Test
    @DisplayName("Should execute raw SQL query")
    void testRawSQL() throws QueryException {

        QueryResult result = database.query()
                .sql("SELECT COUNT(*) as count FROM users WHERE age > ?")
                .param(25)
                .execute();

        Map<String, Object> row = result.getFirst().orElseThrow();
        assertThat(row.get("count")).isEqualTo(2);

    }

    @Test
    @DisplayName("Should support method chaining")
    void testMethodChaining() throws QueryException {

        QueryBuilder builder = database.query()
                .select("users")
                .where("age > ?")
                .param(20)
                .orderBy("name")
                .limit(10);

        assertThat(builder).isNotNull();
        QueryResult result = builder.execute();
        assertThat(result).isNotNull();

    }

    @Test
    @DisplayName("Should handle empty parameter list")
    void testNoParameters() throws QueryException {

        QueryResult result = database.query()
                .sql("SELECT * FROM users")
                .execute();

        assertThat(result.size()).isEqualTo(3);

    }

    @Test
    @DisplayName("Should throw exception for empty SQL")
    void testEmptySQLException() {
        assertThatThrownBy(() -> database.query().execute())
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("SQL query is empty");
    }

    @Test
    @DisplayName("Should throw exception for blank SQL")
    void testBlankSQLException() {
        assertThatThrownBy(() -> database.query().sql("   ").execute())
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("SQL query is empty");
    }

    @Test
    @DisplayName("Should handle NULL parameters")
    void testNullParameters() throws QueryException {

        database.execute(
                "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                "Eve", null, 40
        );

        QueryResult result = database.query()
                .select("users")
                .where("email IS NULL")
                .execute();

        assertThat(result.size()).isEqualTo(1);
        Map<String, Object> row = result.getFirst().orElseThrow();
        assertThat(row.get("name")).isEqualTo("Eve");

    }

    @Test
    @DisplayName("Should handle special characters in data")
    void testSpecialCharacters() throws QueryException {

        Map<String, Object> data = Map.of(
                "name", "O'Brien",
                "email", "o'brien@test.com",
                "age", 45
        );

        database.query().insert("users", data).execute();

        QueryResult result = database.query()
                .select("users")
                .where("name = ?")
                .param("O'Brien")
                .execute();

        assertThat(result.size()).isEqualTo(1);

    }

    @Test
    @DisplayName("Should handle multiple ORDER BY columns")
    void testMultipleOrderBy() throws QueryException {

        database.execute(
                "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                "Alice", "alice2@test.com", 30
        );

        QueryResult result = database.query()
                .select("users")
                .orderBy("age", "name")
                .execute();

        assertThat(result.size()).isEqualTo(4);

    }

    @Test
    @DisplayName("Should update multiple rows")
    void testUpdateMultipleRows() throws QueryException {

        Map<String, Object> data = Map.of("email", "updated@test.com");

        QueryResult result = database.query()
                .update("users", data)
                .where("age >= ?")
                .param(30)
                .execute();

        assertThat(result.getUpdateCount()).isEqualTo(2);

    }

    @Test
    @DisplayName("Should delete multiple rows")
    void testDeleteMultipleRows() throws QueryException {

        QueryResult result = database.query()
                .delete("users")
                .where("age > ?")
                .param(25)
                .execute();

        assertThat(result.getUpdateCount()).isEqualTo(2);

    }

    @Test
    @DisplayName("Should handle WHERE with complex conditions")
    void testComplexWhereConditions() throws QueryException {

        QueryResult result = database.query()
                .select("users")
                .where("(age > ? AND age < ?) OR name = ?")
                .params(25, 35, "Alice")
                .execute();

        assertThat(result.size()).isEqualTo(2);

    }

    @Test
    @DisplayName("Should execute INSERT and verify with SELECT")
    void testInsertAndSelect() throws QueryException {

        Map<String, Object> newUser = Map.of(
                "name", "Frank",
                "email", "frank@test.com",
                "age", 50
        );

        database.query().insert("users", newUser).execute();

        QueryResult result = database.query()
                .select("users")
                .where("age = ?")
                .param(50)
                .execute();

        assertThat(result.size()).isEqualTo(1);
        Map<String, Object> row = result.getFirst().orElseThrow();
        assertThat(row.get("name")).isEqualTo("Frank");

    }

    @Test
    @DisplayName("Should handle numeric parameters correctly")
    void testNumericParameters() throws QueryException {

        QueryResult result = database.query()
                .select("users")
                .where("age = ?")
                .param(30)
                .execute();

        assertThat(result.size()).isEqualTo(1);

    }

    @Test
    @DisplayName("Should handle string parameters with wildcards")
    void testWildcardParameters() throws QueryException {

        QueryResult result = database.query()
                .select("users")
                .where("name LIKE ?")
                .param("%li%")
                .execute();

        assertThat(result.size()).isEqualTo(2); // Alice and Charlie

    }

    @Test
    @DisplayName("Should execute in transaction")
    void testExecuteInTransaction() throws Exception {

        Map<String, Object> data = Map.of(
                "name", "Grace",
                "email", "grace@test.com",
                "age", 27
        );

        QueryResult result = database.query()
                .insert("users", data)
                .executeInTransaction();

        assertThat(result.isUpdate()).isTrue();
        assertThat(result.getUpdateCount()).isEqualTo(1);

        QueryResult verify = database.query().select("users").execute();
        assertThat(verify.size()).isEqualTo(4);

    }

    @Test
    @DisplayName("Should handle INSERT with LinkedHashMap to preserve order")
    void testInsertWithOrderedMap() throws QueryException {

        Map<String, Object> data = new java.util.LinkedHashMap<>();
        data.put("name", "Helen");
        data.put("email", "helen@test.com");
        data.put("age", 32);

        QueryResult result = database.query().insert("users", data).execute();

        assertThat(result.getUpdateCount()).isEqualTo(1);

    }

}