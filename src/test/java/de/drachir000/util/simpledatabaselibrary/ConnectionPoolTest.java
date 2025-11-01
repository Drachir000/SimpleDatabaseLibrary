package de.drachir000.util.simpledatabaselibrary;

import de.drachir000.util.simpledatabaselibrary.configuration.DatabaseConfig;
import de.drachir000.util.simpledatabaselibrary.configuration.DatabaseType;
import de.drachir000.util.simpledatabaselibrary.exception.ConnectionException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("ConnectionPool Tests")
class ConnectionPoolTest {

    private String uniqueDbFile() {
        return "test_pool_" + System.currentTimeMillis() + ".db";
    }

    @Test
    @DisplayName("Should initialize and acquire a connection")
    void testInitializationAndAcquire() throws Exception {

        String dbPath = uniqueDbFile();
        try {
            DatabaseConfig config = DatabaseConfig.builder()
                    .type(DatabaseType.SQLITE)
                    .database(dbPath)
                    .poolSize(2);

            ConnectionPool pool = new ConnectionPool(config);

            assertThat(pool.getConfig()).isEqualTo(config);
            assertThat(pool.getUrl()).isEqualTo(config.buildUrl());

            Connection conn = pool.acquire();
            assertThat(conn).isNotNull();
            assertThat(conn.isClosed()).isFalse();

            pool.release(conn);
            pool.close();
        } finally {
            new File(dbPath).delete();
        }

    }

    @Test
    @DisplayName("Should create new connections beyond initial pool size")
    void testDynamicConnectionCreation() throws Exception {

        String dbPath = uniqueDbFile();
        try {
            DatabaseConfig config = DatabaseConfig.builder()
                    .type(DatabaseType.SQLITE)
                    .database(dbPath)
                    .poolSize(1);

            ConnectionPool pool = new ConnectionPool(config);

            Connection c1 = pool.acquire();
            Connection c2 = pool.acquire();

            assertThat(c2).isNotSameAs(c1);

            pool.release(c1);
            pool.release(c2);
            pool.close();
        } finally {
            new File(dbPath).delete();
        }

    }

    @Test
    @DisplayName("Should reuse released connection instance")
    void testReleaseAndReuse() throws Exception {

        String dbPath = uniqueDbFile();
        try {
            DatabaseConfig config = DatabaseConfig.builder()
                    .type(DatabaseType.SQLITE)
                    .database(dbPath)
                    .autoCommit(false)
                    .poolSize(1);

            ConnectionPool pool = new ConnectionPool(config);

            Connection c1 = pool.acquire();
            pool.release(c1);
            Connection c2 = pool.acquire();

            assertThat(c2).isSameAs(c1);

            pool.release(c2);
            pool.close();
        } finally {
            new File(dbPath).delete();
        }

    }

    @Test
    @DisplayName("Should throw when acquiring after pool is closed")
    void testAcquireAfterClose() throws Exception {

        String dbPath = uniqueDbFile();
        try {
            DatabaseConfig config = DatabaseConfig.builder()
                    .type(DatabaseType.SQLITE)
                    .database(dbPath);

            ConnectionPool pool = new ConnectionPool(config);
            pool.close();

            assertThatThrownBy(pool::acquire)
                    .isInstanceOf(ConnectionException.class)
                    .hasMessageContaining("Connection pool is closed");
        } finally {
            new File(dbPath).delete();
        }

    }

    @Test
    @DisplayName("Should handle releasing an externally closed connection")
    void testReleaseClosedConnection() throws Exception {

        String dbPath = uniqueDbFile();
        try {
            DatabaseConfig config = DatabaseConfig.builder()
                    .type(DatabaseType.SQLITE)
                    .database(dbPath)
                    .poolSize(1);

            ConnectionPool pool = new ConnectionPool(config);

            Connection c1 = pool.acquire();
            c1.close(); // close externally
            pool.release(c1); // should not throw

            Connection c2 = pool.acquire();
            assertThat(c2.isClosed()).isFalse();

            pool.release(c2);
            pool.close();
        } finally {
            new File(dbPath).delete();
        }

    }

    @Test
    @DisplayName("Should replace closed available connection on acquire")
    void testReplaceClosedAvailableConnection() throws Exception {

        String dbPath = uniqueDbFile();
        try {
            DatabaseConfig config = DatabaseConfig.builder()
                    .type(DatabaseType.SQLITE)
                    .database(dbPath)
                    .poolSize(1);

            ConnectionPool pool = new ConnectionPool(config);

            Connection c1 = pool.acquire();
            pool.release(c1); // now c1 is available
            c1.close(); // externally close the available connection

            Connection c2 = pool.acquire();
            assertThat(c2).isNotSameAs(c1);
            assertThat(c2.isClosed()).isFalse();

            pool.release(c2);
            pool.close();
        } finally {
            new File(dbPath).delete();
        }

    }

    @Test
    @DisplayName("Should rollback uncommitted changes on release when autoCommit=false")
    void testRollbackOnReleaseWithAutoCommitFalse() throws Exception {

        String dbPath = uniqueDbFile();
        try {
            DatabaseConfig config = DatabaseConfig.builder()
                    .type(DatabaseType.SQLITE)
                    .database(dbPath)
                    .autoCommit(false)
                    .poolSize(1);

            ConnectionPool pool = new ConnectionPool(config);

            // Use a single connection to create schema and then insert without commit
            Connection conn = pool.acquire();

            // Ensure schema is committed
            conn.setAutoCommit(true);
            try (PreparedStatement st = conn.prepareStatement("CREATE TABLE t (id INTEGER)")) {
                st.execute();
            }
            // Now start a transaction (autoCommit=false)
            conn.setAutoCommit(false);
            try (PreparedStatement st = conn.prepareStatement("INSERT INTO t (id) VALUES (1)")) {
                st.executeUpdate();
            }

            // Release without commit -> pool will call rollback()
            pool.release(conn);

            // Verify that row was rolled back
            Connection verify = pool.acquire();
            verify.setAutoCommit(true);
            try (PreparedStatement st = verify.prepareStatement("SELECT COUNT(*) FROM t");
                 ResultSet rs = st.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isZero();
            }
            pool.release(verify);
            pool.close();
        } finally {
            new File(dbPath).delete();
        }

    }

}
