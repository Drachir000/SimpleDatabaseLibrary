package de.drachir000.util.simpledatabaselibrary;

import de.drachir000.util.simpledatabaselibrary.configuration.DatabaseConfig;
import de.drachir000.util.simpledatabaselibrary.exception.ConnectionException;
import lombok.Getter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple connection pool implementation.
 * Manages a pool of database connections for reuse.
 */
class ConnectionPool {

    private final Queue<Connection> available = new LinkedList<>();
    private final Set<Connection> inUse = ConcurrentHashMap.newKeySet();
    @Getter
    private final DatabaseConfig config;
    @Getter
    private final String url;
    @Getter
    private boolean closed = false;

    /**
     * Creates a new connection pool.
     *
     * @param config database configuration
     * @throws ConnectionException if pool initialization fails
     */
    ConnectionPool(DatabaseConfig config) throws ConnectionException {

        this.config = config;

        try {

            this.url = config.buildUrl();

            Class.forName(config.getType().getDriverClass());

            for (int i = 0; i < config.getPoolSize(); i++) {
                available.offer(createConnection());
            }

        } catch (Exception e) {
            throw new ConnectionException("Failed to initialize connection pool", e);
        }

    }

    private Connection createConnection() throws SQLException {

        Properties props = new Properties();

        if (config.getUsername() != null) {
            props.setProperty("user", config.getUsername());
        }

        if (config.getPassword() != null) {
            props.setProperty("password", config.getPassword());
        }

        config.getProperties().forEach(props::setProperty);

        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(config.isAutoCommit());

        return conn;

    }

    /**
     * Acquires a connection from the pool.
     *
     * @return database connection
     * @throws ConnectionException if no connection is available
     */
    synchronized Connection acquire() throws ConnectionException {

        if (closed) {
            throw new ConnectionException("Connection pool is closed", null);
        }

        Connection conn = available.poll();
        if (conn == null) {
            try {
                conn = createConnection();
            } catch (SQLException e) {
                throw new ConnectionException("Failed to create connection", e);
            }
        }

        try {
            if (conn.isClosed()) {
                conn = createConnection();
            }
        } catch (SQLException e) {
            throw new ConnectionException("Failed to validate connection", e);
        }

        inUse.add(conn);

        return conn;

    }

    /**
     * Returns a connection to the pool.
     *
     * @param conn connection to release
     */
    synchronized void release(Connection conn) {

        if (conn != null && inUse.remove(conn)) {
            try {

                if (!conn.isClosed()) {
                    conn.rollback();
                    available.offer(conn);
                }

            } catch (SQLException e) {

                try {
                    conn.close();
                } catch (SQLException ignored) {
                }

            }
        }

    }

    /**
     * Closes all connections in the pool.
     */
    synchronized void close() {

        closed = true;

        available.forEach(conn -> {
            try {
                conn.close();
            } catch (SQLException ignored) {
            }
        });
        available.clear();

        inUse.forEach(conn -> {
            try {
                conn.close();
            } catch (SQLException ignored) {
            }
        });
        inUse.clear();

    }

}
