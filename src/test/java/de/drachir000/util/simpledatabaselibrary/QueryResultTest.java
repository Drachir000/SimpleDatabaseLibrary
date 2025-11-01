package de.drachir000.util.simpledatabaselibrary;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.when;

/**
 * Comprehensive tests for QueryResult.
 * Tests for result mapping, transformation, and data access methods.
 */
@DisplayName("QueryResult Tests")
class QueryResultTest {

    @Mock
    private ResultSet resultSet;

    @Mock
    private ResultSetMetaData metaData;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    @DisplayName("Should create QueryResult from empty ResultSet")
    void testEmptyResultSet() throws SQLException {

        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(0);
        when(resultSet.next()).thenReturn(false);

        QueryResult result = QueryResult.fromResultSet(resultSet);

        assertThat(result.isEmpty()).isTrue();
        assertThat(result.size()).isZero();
        assertThat(result.getRows()).isEmpty();
        assertThat(result.isUpdate()).isFalse();

    }

    @Test
    @DisplayName("Should create QueryResult from ResultSet with single row")
    void testSingleRowResultSet() throws SQLException {

        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(2);
        when(metaData.getColumnName(1)).thenReturn("id");
        when(metaData.getColumnName(2)).thenReturn("name");
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getObject(1)).thenReturn(1);
        when(resultSet.getObject(2)).thenReturn("John");

        QueryResult result = QueryResult.fromResultSet(resultSet);

        assertThat(result.isEmpty()).isFalse();
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.getRows()).hasSize(1);

        Map<String, Object> row = result.getFirst().orElseThrow();
        assertThat(row).containsEntry("id", 1)
                .containsEntry("name", "John");

    }

    @Test
    @DisplayName("Should create QueryResult from ResultSet with multiple rows")
    void testMultipleRowsResultSet() throws SQLException {

        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(3);
        when(metaData.getColumnName(1)).thenReturn("id");
        when(metaData.getColumnName(2)).thenReturn("name");
        when(metaData.getColumnName(3)).thenReturn("age");

        when(resultSet.next()).thenReturn(true, true, true, false);
        when(resultSet.getObject(1)).thenReturn(1, 2, 3);
        when(resultSet.getObject(2)).thenReturn("Alice", "Bob", "Charlie");
        when(resultSet.getObject(3)).thenReturn(25, 30, 35);

        QueryResult result = QueryResult.fromResultSet(resultSet);

        assertThat(result.size()).isEqualTo(3);
        assertThat(result.isEmpty()).isFalse();

        List<Map<String, Object>> rows = result.getRows();
        assertThat(rows.get(0)).containsEntry("name", "Alice");
        assertThat(rows.get(1)).containsEntry("name", "Bob");
        assertThat(rows.get(2)).containsEntry("name", "Charlie");

    }

    @Test
    @DisplayName("Should create update result from count")
    void testUpdateResult() {

        QueryResult result = QueryResult.fromUpdateCount(5);

        assertThat(result.isUpdate()).isTrue();
        assertThat(result.getUpdateCount()).isEqualTo(5);
        assertThat(result.isEmpty()).isTrue();
        assertThat(result.size()).isZero();

    }

    @Test
    @DisplayName("Should handle zero update count")
    void testZeroUpdateCount() {

        QueryResult result = QueryResult.fromUpdateCount(0);

        assertThat(result.isUpdate()).isTrue();
        assertThat(result.getUpdateCount()).isZero();

    }

    @Test
    @DisplayName("Should return empty optional for empty result")
    void testGetFirstOnEmpty() throws SQLException {

        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(0);
        when(resultSet.next()).thenReturn(false);

        QueryResult result = QueryResult.fromResultSet(resultSet);

        assertThat(result.getFirst()).isEmpty();

    }

    @Test
    @DisplayName("Should return first row when multiple exist")
    void testGetFirstWithMultipleRows() throws SQLException {

        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnName(1)).thenReturn("value");
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getObject(1)).thenReturn("first", "second");

        QueryResult result = QueryResult.fromResultSet(resultSet);

        Map<String, Object> first = result.getFirst().orElseThrow();
        assertThat(first.get("value")).isEqualTo("first");

    }

    @Test
    @DisplayName("Should map results to custom objects")
    void testMap() throws SQLException {

        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(2);
        when(metaData.getColumnName(1)).thenReturn("name");
        when(metaData.getColumnName(2)).thenReturn("age");
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getObject(1)).thenReturn("Alice", "Bob");
        when(resultSet.getObject(2)).thenReturn(25, 30);

        QueryResult result = QueryResult.fromResultSet(resultSet);

        List<String> names = result.map(row -> (String) row.get("name"));

        assertThat(names).containsExactly("Alice", "Bob");

    }

    @Test
    @DisplayName("Should map empty result to empty list")
    void testMapEmpty() throws SQLException {

        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(0);
        when(resultSet.next()).thenReturn(false);

        QueryResult result = QueryResult.fromResultSet(resultSet);

        List<String> mapped = result.map(row -> "test");

        assertThat(mapped).isEmpty();

    }

    @Test
    @DisplayName("Should extract column values")
    void testGetColumn() throws SQLException {

        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(2);
        when(metaData.getColumnName(1)).thenReturn("id");
        when(metaData.getColumnName(2)).thenReturn("score");
        when(resultSet.next()).thenReturn(true, true, true, false);
        when(resultSet.getObject(1)).thenReturn(1, 2, 3);
        when(resultSet.getObject(2)).thenReturn(95, 87, 92);

        QueryResult result = QueryResult.fromResultSet(resultSet);

        List<Object> scores = result.getColumn("score");

        assertThat(scores).containsExactly(95, 87, 92);

    }

    @Test
    @DisplayName("Should handle null values in columns")
    void testNullValuesInColumn() throws SQLException {

        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnName(1)).thenReturn("value");
        when(resultSet.next()).thenReturn(true, true, true, false);
        when(resultSet.getObject(1)).thenReturn("A", null, "C");

        QueryResult result = QueryResult.fromResultSet(resultSet);

        List<Object> values = result.getColumn("value");

        assertThat(values).containsExactly("A", null, "C");

    }

    @Test
    @DisplayName("Should return empty list for non-existent column")
    void testGetNonExistentColumn() throws SQLException {

        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnName(1)).thenReturn("existing");
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getObject(1)).thenReturn("value");

        QueryResult result = QueryResult.fromResultSet(resultSet);

        List<Object> values = result.getColumn("nonexistent");

        assertThat(values).containsExactly((Object) null);

    }

    @Test
    @DisplayName("Should return defensive copy of rows")
    void testGetRowsDefensiveCopy() throws SQLException {

        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnName(1)).thenReturn("id");
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getObject(1)).thenReturn(1);

        QueryResult result = QueryResult.fromResultSet(resultSet);

        List<Map<String, Object>> rows = result.getRows();
        rows.clear();

        assertThat(result.getRows()).hasSize(1);

    }

    @Test
    @DisplayName("Should handle different data types")
    void testDifferentDataTypes() throws SQLException {

        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(5);
        when(metaData.getColumnName(1)).thenReturn("intVal");
        when(metaData.getColumnName(2)).thenReturn("stringVal");
        when(metaData.getColumnName(3)).thenReturn("doubleVal");
        when(metaData.getColumnName(4)).thenReturn("boolVal");
        when(metaData.getColumnName(5)).thenReturn("nullVal");

        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getObject(1)).thenReturn(42);
        when(resultSet.getObject(2)).thenReturn("test");
        when(resultSet.getObject(3)).thenReturn(3.14);
        when(resultSet.getObject(4)).thenReturn(true);
        when(resultSet.getObject(5)).thenReturn(null);

        QueryResult result = QueryResult.fromResultSet(resultSet);

        Map<String, Object> row = result.getFirst().orElseThrow();
        assertThat(row.get("intVal")).isEqualTo(42);
        assertThat(row.get("stringVal")).isEqualTo("test");
        assertThat(row.get("doubleVal")).isEqualTo(3.14);
        assertThat(row.get("boolVal")).isEqualTo(true);
        assertThat(row.get("nullVal")).isNull();

    }

    @Test
    @DisplayName("Should maintain column order")
    void testColumnOrder() throws SQLException {

        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(3);
        when(metaData.getColumnName(1)).thenReturn("first");
        when(metaData.getColumnName(2)).thenReturn("second");
        when(metaData.getColumnName(3)).thenReturn("third");
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getObject(anyInt())).thenReturn(null);

        QueryResult result = QueryResult.fromResultSet(resultSet);

        Map<String, Object> row = result.getFirst().orElseThrow();
        List<String> keys = List.copyOf(row.keySet());

        assertThat(keys).containsExactly("first", "second", "third");

    }

    @Test
    @DisplayName("Should handle large result sets")
    void testLargeResultSet() throws SQLException {

        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(1);
        when(metaData.getColumnName(1)).thenReturn("id");

        Boolean[] nexts = new Boolean[1001];
        for (int i = 0; i < 1000; i++) {
            nexts[i] = true;
        }
        nexts[1000] = false;
        when(resultSet.next()).thenReturn(true, nexts);
        when(resultSet.getObject(1)).thenReturn(1);

        QueryResult result = QueryResult.fromResultSet(resultSet);

        assertThat(result.size()).isEqualTo(1000);

    }

    @Test
    @DisplayName("Should handle special characters in column names")
    void testSpecialCharactersInColumnNames() throws SQLException {

        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(2);
        when(metaData.getColumnName(1)).thenReturn("column_name");
        when(metaData.getColumnName(2)).thenReturn("Column-Name-2");
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getObject(1)).thenReturn("value1");
        when(resultSet.getObject(2)).thenReturn("value2");

        QueryResult result = QueryResult.fromResultSet(resultSet);

        Map<String, Object> row = result.getFirst().orElseThrow();
        assertThat(row).containsKey("column_name");
        assertThat(row).containsKey("Column-Name-2");

    }

    @Test
    @DisplayName("Should handle complex mapping scenarios")
    void testComplexMapping() throws SQLException {

        when(resultSet.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(3);
        when(metaData.getColumnName(1)).thenReturn("firstName");
        when(metaData.getColumnName(2)).thenReturn("lastName");
        when(metaData.getColumnName(3)).thenReturn("age");
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getObject(1)).thenReturn("John", "Jane");
        when(resultSet.getObject(2)).thenReturn("Doe", "Smith");
        when(resultSet.getObject(3)).thenReturn(30, 25);

        QueryResult result = QueryResult.fromResultSet(resultSet);

        List<String> fullNames = result.map(row ->
                row.get("firstName") + " " + row.get("lastName")
        );

        assertThat(fullNames).containsExactly("John Doe", "Jane Smith");

    }

}