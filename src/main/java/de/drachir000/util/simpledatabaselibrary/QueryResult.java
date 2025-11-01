package de.drachir000.util.simpledatabaselibrary;

import lombok.Getter;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents the result of a database query.
 * Provides methods to access and transform result data.
 */
class QueryResult {

    private final List<Map<String, Object>> rows;
    @Getter
    private final int updateCount;
    @Getter
    private final boolean isUpdate;

    private QueryResult(List<Map<String, Object>> rows) {
        this.rows = rows;
        this.updateCount = -1;
        this.isUpdate = false;
    }

    private QueryResult(int updateCount) {
        this.rows = Collections.emptyList();
        this.updateCount = updateCount;
        this.isUpdate = true;
    }

    static QueryResult fromResultSet(ResultSet rs) throws SQLException {

        List<Map<String, Object>> rows = new ArrayList<>();
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        while (rs.next()) {

            Map<String, Object> row = new LinkedHashMap<>();

            for (int i = 1; i <= columnCount; i++) {
                row.put(meta.getColumnName(i), rs.getObject(i));
            }

            rows.add(row);

        }

        return new QueryResult(rows);

    }

    static QueryResult fromUpdateCount(int count) {
        return new QueryResult(count);
    }

    /**
     * Gets all rows as a list of maps.
     *
     * @return list of row data
     */
    public List<Map<String, Object>> getRows() {
        return new ArrayList<>(rows);
    }

    /**
     * Gets the first row, or empty Optional if no results.
     *
     * @return first row
     */
    public Optional<Map<String, Object>> getFirst() {
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }

    /**
     * Gets the number of rows returned.
     *
     * @return row count
     */
    public int size() {
        return rows.size();
    }

    /**
     * Checks if the result is empty.
     *
     * @return true if no rows
     */
    public boolean isEmpty() {
        return rows.isEmpty();
    }

    /**
     * Maps results to a list of objects using the provided mapper.
     *
     * @param mapper function to convert row to object
     * @param <T>    result type
     * @return list of mapped objects
     */
    public <T> List<T> map(Function<Map<String, Object>, T> mapper) {
        return rows.stream().map(mapper).collect(Collectors.toList());
    }

    /**
     * Gets a specific column from all rows.
     *
     * @param columnName column name
     * @return list of column values
     */
    public List<Object> getColumn(String columnName) {
        return rows.stream()
                .map(row -> row.get(columnName))
                .collect(Collectors.toList());
    }

}
