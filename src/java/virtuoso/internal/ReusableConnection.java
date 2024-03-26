package virtuoso.internal;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

import clojure.lang.IDeref;

/*
 * A wrapper class around `java.sql.Connection` (further wrapped in a `clojure.lang.Delay`).
 * All methods delegate to the underlying Connection , except `.close()`, `.isWrapperFor()`
 * and `.unwrap()`.
 *
 */
public class ReusableConnection implements Connection {

    private final IDeref    delegate;
    private final Instant   createdAt;
    private final Semaphore isBusy;

    public ReusableConnection(IDeref connection) {
        delegate = connection;
        isBusy = new Semaphore(1);
        createdAt = Instant.now();
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setBusy() throws InterruptedException {
        isBusy.acquire();
    }

    @Override
    public void close() throws SQLException{
        isBusy.release();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        ((Connection) delegate.deref()).abort(executor);
    }

    @Override
    public void clearWarnings() throws SQLException {
        ((Connection) delegate.deref()).clearWarnings();
    }

    @Override
    public void commit() throws SQLException {
        ((Connection) delegate.deref()).commit();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return ((Connection) delegate.deref()).createArrayOf(typeName, elements);
    }

    @Override
    public Blob createBlob() throws SQLException {
        return ((Connection) delegate.deref()).createBlob();
    }

    @Override
    public Clob createClob() throws SQLException {
        return ((Connection) delegate.deref()).createClob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return ((Connection) delegate.deref()).createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return ((Connection) delegate.deref()).createSQLXML();
    }

    @Override
    public Statement createStatement() throws SQLException {
        return ((Connection) delegate.deref()).createStatement();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return ((Connection) delegate.deref()).createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return ((Connection) delegate.deref()).createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return ((Connection) delegate.deref()).createStruct(typeName, attributes);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return ((Connection) delegate.deref()).getAutoCommit();
    }

    @Override
    public String getCatalog() throws SQLException {
        return ((Connection) delegate.deref()).getCatalog();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return ((Connection) delegate.deref()).getClientInfo();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return ((Connection) delegate.deref()).getClientInfo(name);
    }

    @Override
    public int getHoldability() throws SQLException {
        return ((Connection) delegate.deref()).getHoldability();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return ((Connection) delegate.deref()).getMetaData();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return ((Connection) delegate.deref()).getNetworkTimeout();
    }

    @Override
    public String getSchema() throws SQLException {
        return ((Connection) delegate.deref()).getSchema();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return ((Connection) delegate.deref()).getTransactionIsolation();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return ((Connection) delegate.deref()).getTypeMap();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return ((Connection) delegate.deref()).getWarnings();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return ((Connection) delegate.deref()).isClosed();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return ((Connection) delegate.deref()).isReadOnly();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return ((Connection) delegate.deref()).isValid(timeout);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return ((Connection) delegate.deref()).nativeSQL(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return ((Connection) delegate.deref()).prepareCall(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return ((Connection) delegate.deref()).prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        return ((Connection) delegate.deref()).prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return ((Connection) delegate.deref()).prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return ((Connection) delegate.deref()).prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return ((Connection) delegate.deref()).prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return ((Connection) delegate.deref()).prepareStatement(sql, columnNames);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return ((Connection) delegate.deref()).prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        return ((Connection) delegate.deref()).prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        ((Connection) delegate.deref()).releaseSavepoint(savepoint);
    }

    @Override
    public void rollback() throws SQLException {
        ((Connection) delegate.deref()).rollback();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        ((Connection) delegate.deref()).rollback(savepoint);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        ((Connection) delegate.deref()).setAutoCommit(autoCommit);
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        ((Connection) delegate.deref()).setCatalog(catalog);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        ((Connection) delegate.deref()).setClientInfo(properties);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        ((Connection) delegate.deref()).setClientInfo(name, value);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        ((Connection) delegate.deref()).setHoldability(holdability);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        ((Connection) delegate.deref()).setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        ((Connection) delegate.deref()).setReadOnly(readOnly);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return ((Connection) delegate.deref()).setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return ((Connection) delegate.deref()).setSavepoint(name);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        ((Connection) delegate.deref()).setSchema(schema);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        ((Connection) delegate.deref()).setTransactionIsolation(level);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        ((Connection) delegate.deref()).setTypeMap(map);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface == Connection.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return (T) delegate.deref();
    }

}
