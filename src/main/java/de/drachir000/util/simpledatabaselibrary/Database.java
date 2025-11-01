package de.drachir000.util.simpledatabaselibrary;

import de.drachir000.util.simpledatabaselibrary.configuration.DatabaseConfig;
import de.drachir000.util.simpledatabaselibrary.exception.ConnectionException;
import de.drachir000.util.simpledatabaselibrary.exception.DatabaseException;
import de.drachir000.util.simpledatabaselibrary.exception.QueryException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Main entry point for database operations.
 * Provides a high-level API for executing queries and managing transactions.
 */
public class Database implements AutoCloseable {

    private final ConnectionPool pool;
    private final DatabaseConfig config;
    private final ThreadLocal<Connection> transactionConnection = new ThreadLocal<>();

    /**
     * Creates a new Database instance with the given configuration.
     *
     * @param config database configuration
     * @throws ConnectionException if the connection fails
     */
    public Database(DatabaseConfig config) throws ConnectionException {
        this.config = config;
        this.pool = new ConnectionPool(config);
    }

    /**
     * Creates a new query builder.
     *
     * @return query builder instance
     */
    public QueryBuilder query() {
        return new QueryBuilder(this);
    }

    /**
     * Executes a raw SQL query with parameters.
     *
     * @param sql    SQL statement
     * @param params query parameters
     * @return query result
     * @throws QueryException if execution fails
     */
    public QueryResult execute(String sql, Object... params) throws QueryException {

        Connection conn = transactionConnection.get();
        boolean releaseConn = false;

        if (conn == null) {
            try {
                conn = pool.acquire();
                releaseConn = true;
            } catch (ConnectionException e) {
                throw new QueryException("Failed to acquire connection", e);
            }
        }

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {

            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }

            boolean isResultSet = stmt.execute();

            if (isResultSet) {
                try (ResultSet rs = stmt.getResultSet()) {
                    return QueryResult.fromResultSet(rs);
                }
            } else {
                return QueryResult.fromUpdateCount(stmt.getUpdateCount());
            }

        } catch (SQLException e) {
            throw new QueryException("Query execution failed: " + sql, e);
        } finally {
            if (releaseConn) {
                pool.release(conn);
            }
        }
    }

    /**
     * Executes multiple operations in a transaction.
     * Automatically commits on success or rolls back on failure.
     *
     * @param operation operations to execute
     * @param <T>       return type
     * @return operation result
     * @throws DatabaseException if transaction fails
     */
    public <T> T transaction(TransactionOperation<T> operation) throws DatabaseException {

        if (transactionConnection.get() != null) {

            // Already in transaction, just execute
            try {
                return operation.execute();
            } catch (Exception e) {
                throw new DatabaseException("Transaction operation failed", e);
            }

        }

        Connection conn = pool.acquire();
        boolean originalAutoCommit = config.isAutoCommit();

        try {

            conn.setAutoCommit(false);
            transactionConnection.set(conn);

            T result = operation.execute();

            conn.commit();

            return result;

        } catch (Exception e) {

            try {
                conn.rollback();
            } catch (SQLException rollbackEx) {
                e.addSuppressed(rollbackEx);
            }

            throw new DatabaseException("Transaction failed", e);

        } finally {

            transactionConnection.remove();

            try {
                conn.setAutoCommit(originalAutoCommit);
            } catch (SQLException ignored) {
            }

            pool.release(conn);

        }

    }

    /**
     * Executes a SELECT query.
     *
     * @param table table name
     * @return query result
     * @throws QueryException if execution fails
     */
    public QueryResult select(String table) throws QueryException {
        return query().select(table).execute();
    }

    /**
     * Inserts a row into a table.
     *
     * @param table table name
     * @param data  column-value pairs
     * @return query result with update count
     * @throws QueryException if execution fails
     */
    public QueryResult insert(String table, Map<String, Object> data) throws QueryException {
        return query().insert(table, data).execute();
    }

    /**
     * Updates rows in a table.
     *
     * @param table       table name
     * @param data        column-value pairs to update
     * @param whereClause WHERE condition
     * @param whereParams parameters for WHERE clause
     * @return query result with update count
     * @throws QueryException if execution fails
     */
    public QueryResult update(String table, Map<String, Object> data, String whereClause, Object... whereParams) throws QueryException {
        return query().update(table, data).where(whereClause).params(whereParams).execute();
    }

    /**
     * Deletes rows from a table.
     *
     * @param table       table name
     * @param whereClause WHERE condition
     * @param whereParams parameters for WHERE clause
     * @return query result with update count
     * @throws QueryException if execution fails
     */
    public QueryResult delete(String table, String whereClause, Object... whereParams) throws QueryException {
        return query().delete(table).where(whereClause).params(whereParams).execute();
    }

    @Override
    public void close() {
        pool.close();
    }

    /**
     * Functional interface for transaction operations.
     *
     * @param <T> return type
     */
    @FunctionalInterface
    public interface TransactionOperation<T> {
        T execute() throws Exception;
    }

}
