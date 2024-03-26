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

import clojure.lang.Delay;

/*
 * A wrapper class around `java.sql.Connection` (further wrapped in a `clojure.lang.Delay`).
 * All methods delegate to the underlying Connection , except `.close()`, `.isWrapperFor()`
 * and `.unwrap()`.
 *
 */
public class ReusableConnection implements Connection {

    private final Delay    delegate;
    private       Instant  createdAt;
    private final Semaphore isBusy;
    
    private static Connection delayed(ReusableConnection rc){
        Delay d = rc.getDelegate();
        if (rc.getCreatedAt() == null)
            rc.setCreatedAt();
      return ((Connection) d.deref());  
    }

    public ReusableConnection(Delay connection) {
        delegate = connection;
        isBusy = new Semaphore(1);
        //createdAt = Instant.now();
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    private Delay getDelegate(){
        return delegate;
    }

    public void setCreatedAt() {
        createdAt = Instant.now();
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
        ReusableConnection.delayed(this).abort(executor);
    }

    @Override
    public void clearWarnings() throws SQLException {
        ReusableConnection.delayed(this).clearWarnings();
    }

    @Override
    public void commit() throws SQLException {
        ReusableConnection.delayed(this).commit();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return ReusableConnection.delayed(this).createArrayOf(typeName, elements);
    }

    @Override
    public Blob createBlob() throws SQLException {
        return ReusableConnection.delayed(this).createBlob();
    }

    @Override
    public Clob createClob() throws SQLException {
        return ReusableConnection.delayed(this).createClob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return ReusableConnection.delayed(this).createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return ReusableConnection.delayed(this).createSQLXML();
    }

    @Override
    public Statement createStatement() throws SQLException {
        return ReusableConnection.delayed(this).createStatement();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return ReusableConnection.delayed(this).createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return ReusableConnection.delayed(this).createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return ReusableConnection.delayed(this).createStruct(typeName, attributes);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return ReusableConnection.delayed(this).getAutoCommit();
    }

    @Override
    public String getCatalog() throws SQLException {
        return ReusableConnection.delayed(this).getCatalog();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return ReusableConnection.delayed(this).getClientInfo();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return ReusableConnection.delayed(this).getClientInfo(name);
    }

    @Override
    public int getHoldability() throws SQLException {
        return ReusableConnection.delayed(this).getHoldability();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return ReusableConnection.delayed(this).getMetaData();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return ReusableConnection.delayed(this).getNetworkTimeout();
    }

    @Override
    public String getSchema() throws SQLException {
        return ReusableConnection.delayed(this).getSchema();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return ReusableConnection.delayed(this).getTransactionIsolation();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return ReusableConnection.delayed(this).getTypeMap();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return ReusableConnection.delayed(this).getWarnings();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return ReusableConnection.delayed(this).isClosed();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return ReusableConnection.delayed(this).isReadOnly();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return ReusableConnection.delayed(this).isValid(timeout);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return ReusableConnection.delayed(this).nativeSQL(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return ReusableConnection.delayed(this).prepareCall(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return ReusableConnection.delayed(this).prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        return ReusableConnection.delayed(this).prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return ReusableConnection.delayed(this).prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return ReusableConnection.delayed(this).prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return ReusableConnection.delayed(this).prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return ReusableConnection.delayed(this).prepareStatement(sql, columnNames);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return ReusableConnection.delayed(this).prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        return ReusableConnection.delayed(this).prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        ReusableConnection.delayed(this).releaseSavepoint(savepoint);
    }

    @Override
    public void rollback() throws SQLException {
        ReusableConnection.delayed(this).rollback();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        ReusableConnection.delayed(this).rollback(savepoint);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        ReusableConnection.delayed(this).setAutoCommit(autoCommit);
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        ReusableConnection.delayed(this).setCatalog(catalog);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            ReusableConnection.delayed(this).setClientInfo(properties);
        } catch (SQLException e) {
            if (e instanceof SQLClientInfoException)
                throw (SQLClientInfoException)e;
            else throw new RuntimeException(e);
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            ReusableConnection.delayed(this).setClientInfo(name, value);
        } catch (SQLException e) {
            if (e instanceof SQLClientInfoException)
                throw (SQLClientInfoException)e;
            else throw new RuntimeException(e);
        }
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        ReusableConnection.delayed(this).setHoldability(holdability);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        ReusableConnection.delayed(this).setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        ReusableConnection.delayed(this).setReadOnly(readOnly);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return ReusableConnection.delayed(this).setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return ReusableConnection.delayed(this).setSavepoint(name);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        ReusableConnection.delayed(this).setSchema(schema);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        ReusableConnection.delayed(this).setTransactionIsolation(level);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        ReusableConnection.delayed(this).setTypeMap(map);
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
