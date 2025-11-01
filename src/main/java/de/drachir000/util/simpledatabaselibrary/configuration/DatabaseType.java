package de.drachir000.util.simpledatabaselibrary.configuration;

import lombok.Getter;

/**
 * Supported database types.
 */
@Getter
public enum DatabaseType {

    SQLITE("org.sqlite.JDBC", "jdbc:sqlite:"),
    MYSQL("com.mysql.cj.jdbc.Driver", "jdbc:mysql://"),
    POSTGRESQL("org.postgresql.Driver", "jdbc:postgresql://");

    private final String driverClass;
    private final String urlPrefix;

    DatabaseType(String driverClass, String urlPrefix) {
        this.driverClass = driverClass;
        this.urlPrefix = urlPrefix;
    }

}
