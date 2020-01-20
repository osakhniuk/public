package grok_connect.providers;

import java.nio.file.Paths;
import java.sql.*;
import grok_connect.connectors_info.*;


public class OdbcDataProvider extends JdbcDataProvider {
    public OdbcDataProvider() {
        descriptor = new DataSource();
        descriptor.type = "ODBC";
        descriptor.description = "Query database via ODBC";
        descriptor.connectionTemplate = DbCredentials.dbConnectionTemplate;
        descriptor.credentialsTemplate = DbCredentials.dbCredentialsTemplate;
    }

    public Connection getConnection(DataConnection conn) throws ClassNotFoundException, SQLException {
        // TODO Add path for Linux
        String libPath = Paths.get((new java.io.File("").getAbsolutePath()), /*"grok_connect",*/ "lib", "JdbcOdbc.dll").toString();
        System.load(libPath);
        Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
        return DriverManager.getConnection(getConnectionString(conn), conn.credentials.getLogin(), conn.credentials.getPassword());
    }

    public String getConnectionString(DataConnection conn) {
        String port = (conn.getPort() == null) ? "" : ":" + conn.getPort();
        // TODO Pass driver there, example jdbc:odbc:<driver name>
        return "jdbc:odbc://" + conn.getServer() + port + "/" + conn.getDb();
    }
}
