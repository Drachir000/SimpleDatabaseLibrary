package de.drachir000.util.simpledatabaselibrary;

import de.drachir000.util.simpledatabaselibrary.exception.DatabaseException;
import de.drachir000.util.simpledatabaselibrary.exception.QueryException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Fluent API for building and executing SQL queries.
 * Supports parameterized queries and various SQL operations.
 */
public class QueryBuilder {

    private final Database database;
    private String sql;
    private final List<Object> parameters = new ArrayList<>();

    QueryBuilder(Database database) {
        this.database = database;
    }

    /**
     * Sets the raw SQL query.
     *
     * @param sql SQL statement
     * @return this builder for chaining
     */
    public QueryBuilder sql(String sql) {
        this.sql = sql;
        return this;
    }

    /**
     * Adds a parameter for the prepared statement.
     *
     * @param param parameter value
     * @return this builder for chaining
     */
    public QueryBuilder param(Object param) {
        this.parameters.add(param);
        return this;
    }

    /**
     * Adds multiple parameters.
     *
     * @param params parameter values
     * @return this builder for chaining
     */
    public QueryBuilder params(Object... params) {
        Collections.addAll(this.parameters, params);
        return this;
    }

    /**
     * Builds a SELECT query.
     *
     * @param table table name
     * @return this builder for chaining
     */
    public QueryBuilder select(String table) {
        this.sql = "SELECT * FROM " + table;
        return this;
    }

    /**
     * Builds a SELECT query with specific columns.
     *
     * @param table   table name
     * @param columns column names
     * @return this builder for chaining
     */
    public QueryBuilder select(String table, String... columns) {
        this.sql = "SELECT " + String.join(", ", columns) + " FROM " + table;
        return this;
    }

    /**
     * Adds WHERE clause.
     *
     * @param condition WHERE condition
     * @return this builder for chaining
     */
    public QueryBuilder where(String condition) {
        this.sql += " WHERE " + condition;
        return this;
    }

    /**
     * Adds ORDER BY clause.
     *
     * @param columns columns to order by
     * @return this builder for chaining
     */
    public QueryBuilder orderBy(String... columns) {
        this.sql += " ORDER BY " + String.join(", ", columns);
        return this;
    }

    /**
     * Adds LIMIT clause.
     *
     * @param limit maximum rows
     * @return this builder for chaining
     */
    public QueryBuilder limit(int limit) {
        this.sql += " LIMIT " + limit;
        return this;
    }

    /**
     * Builds an INSERT query.
     *
     * @param table table name
     * @param data  column-value pairs
     * @return this builder for chaining
     */
    public QueryBuilder insert(String table, Map<String, Object> data) {

        String columns = String.join(", ", data.keySet());

        String placeholders = String.join(", ", Collections.nCopies(data.size(), "?"));

        this.sql = "INSERT INTO " + table + " (" + columns + ") VALUES (" + placeholders + ")";
        this.parameters.addAll(data.values());

        return this;

    }

    /**
     * Builds an UPDATE query.
     *
     * @param table table name
     * @param data  column-value pairs
     * @return this builder for chaining
     */
    public QueryBuilder update(String table, Map<String, Object> data) {

        String sets = data.keySet().stream()
                .map(col -> col + " = ?")
                .collect(Collectors.joining(", "));
        this.sql = "UPDATE " + table + " SET " + sets;

        this.parameters.addAll(data.values());

        return this;

    }

    /**
     * Builds a DELETE query.
     *
     * @param table table name
     * @return this builder for chaining
     */
    public QueryBuilder delete(String table) {
        this.sql = "DELETE FROM " + table;
        return this;
    }

    /**
     * Executes the query and returns results.
     *
     * @return query result
     * @throws QueryException if execution fails
     */
    public QueryResult execute() throws QueryException {

        if (sql == null || sql.isBlank()) {
            throw new QueryException("SQL query is empty", null);
        }

        return database.execute(sql, parameters.toArray());

    }

    /**
     * Executes the query in a transaction.
     *
     * @return query result
     * @throws QueryException if execution fails
     */
    public QueryResult executeInTransaction() throws QueryException {

        try {
            return database.transaction(this::execute);
        } catch (DatabaseException e) {
            throw new QueryException("Transaction failed", e);
        }

    }

}
