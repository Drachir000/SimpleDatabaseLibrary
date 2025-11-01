package de.drachir000.util.simpledatabaselibrary.configuration;

import de.drachir000.util.simpledatabaselibrary.exception.ConfigurationException;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration builder for database connections.
 * Provides a fluent API for setting connection parameters.
 */
@Getter
public class DatabaseConfig {

    private DatabaseType type;
    private String host;
    private int port;
    private String database;
    private String username;
    private String password;
    private int poolSize = 5;
    private int connectionTimeout = 30000;
    private boolean autoCommit = true;
    private final Map<String, String> properties = new HashMap<>();

    private DatabaseConfig() {
    }

    /**
     * Creates a new configuration builder.
     *
     * @return new DatabaseConfig instance
     */
    public static DatabaseConfig builder() {
        return new DatabaseConfig();
    }

    /**
     * Sets the database type.
     *
     * @param type database type
     * @return this config for chaining
     */
    public DatabaseConfig type(DatabaseType type) {
        this.type = type;
        return this;
    }

    /**
     * Sets the host address.
     *
     * @param host hostname or IP
     * @return this config for chaining
     */
    public DatabaseConfig host(String host) {
        this.host = host;
        return this;
    }

    /**
     * Sets the port number.
     *
     * @param port port number
     * @return this config for chaining
     */
    public DatabaseConfig port(int port) {
        this.port = port;
        return this;
    }

    /**
     * Sets the database name or file path (for SQLite).
     *
     * @param database database name or path
     * @return this config for chaining
     */
    public DatabaseConfig database(String database) {
        this.database = database;
        return this;
    }

    /**
     * Sets the username for authentication.
     *
     * @param username database username
     * @return this config for chaining
     */
    public DatabaseConfig username(String username) {
        this.username = username;
        return this;
    }

    /**
     * Sets the password for authentication.
     *
     * @param password database password
     * @return this config for chaining
     */
    public DatabaseConfig password(String password) {
        this.password = password;
        return this;
    }

    /**
     * Sets the connection pool size.
     *
     * @param poolSize maximum number of connections
     * @return this config for chaining
     */
    public DatabaseConfig poolSize(int poolSize) {
        this.poolSize = poolSize;
        return this;
    }

    /**
     * Sets the connection timeout in milliseconds.
     *
     * @param timeout timeout in ms
     * @return this config for chaining
     */
    public DatabaseConfig connectionTimeout(int timeout) {
        this.connectionTimeout = timeout;
        return this;
    }

    /**
     * Sets auto-commit mode.
     *
     * @param autoCommit true to enable auto-commit
     * @return this config for chaining
     */
    public DatabaseConfig autoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
        return this;
    }

    /**
     * Adds a custom connection property.
     *
     * @param key   property key
     * @param value property value
     * @return this config for chaining
     */
    public DatabaseConfig property(String key, String value) {
        this.properties.put(key, value);
        return this;
    }

    /**
     * Builds the JDBC connection URL.
     *
     * @return JDBC URL string
     * @throws ConfigurationException if the configuration is invalid
     */
    public String buildUrl() throws ConfigurationException {

        if (type == null) {
            throw new ConfigurationException("Database type must be specified");
        }

        if (database == null || database.isBlank()) {
            throw new ConfigurationException("Database name/path must be specified");
        }

        return switch (type) {

            case SQLITE -> type.getUrlPrefix() + database;

            case MYSQL -> {
                if (host == null) host = "localhost";
                if (port == 0) port = 3306;
                yield type.getUrlPrefix() + host + ":" + port + "/" + database;
            }

            case POSTGRESQL -> {
                if (host == null) host = "localhost";
                if (port == 0) port = 5432;
                yield type.getUrlPrefix() + host + ":" + port + "/" + database;
            }

        };

    }

}
