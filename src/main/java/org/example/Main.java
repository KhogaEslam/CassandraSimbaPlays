package org.example;

import com.simba.cassandra.jdbc42.DataSource;
import com.simba.cassandra.support.LogLevel;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Main {
    private static final String DRIVER_CLASS = "com.simba.cassandra.jdbc42.Driver";
    static String username = "";
    static String password = "";
    static String host = "";
    static int port = 9042;
    static String localDataCenter = "";

    private static final String CONNECTION_URL = "jdbc:cassandra://" +
            host +
            ":" +
            port +
            "/" +
            localDataCenter +
            ";" +
            "AuthMech=1" +
            ";UID=" +
            username +
            ";PWD=" +
            password;
//            + ";LogLevel=6;LogPath=/Users/eslam.khoga/Desktop/logs;EnablePaging=0;RowsPerPage=10;TunableConsistency=10;Flags=11111111";
//            +
//            ";cql.query.flags.page_size=false" +
//            ";cql.page_size=321";

    static String query = "SELECT * FROM system.local";
    static String query1 = "SELECT id, address, age, city, gender, name, occupation, zip FROM movielens.users /*page_size=111*/;";
    static int fetchSize = 222;

    public static void main(String[] args) {
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Class.forName("com.simba.cassandra.jdbc42.Driver");

//            Connection con = connectViaDM();
            Connection con = connectViaDS();
            PreparedStatement stmt = con.prepareStatement(query1);
            stmt.setFetchSize(fetchSize);
            stmt.setMaxRows(fetchSize);
            stmt.setPoolable(true);
            stmt.setLargeMaxRows(fetchSize);
            stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                System.out.println(rs);
            }

            rs.close();
            con.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Connection connectViaDM() throws Exception {
        Connection connection = null;
        Class.forName(DRIVER_CLASS);
        connection = DriverManager.getConnection(CONNECTION_URL);
        return connection;
    }

    private static Connection connectViaDS() throws Exception {
        Connection connection = null;
        Class.forName(DRIVER_CLASS);
        DataSource ds = new DataSource();
//        ds.setURL(host + ":" + port);
//        ds.setUserID(username);
//        ds.setPassword(password);
        ds.setLogLevel(LogLevel.TRACE.name());
        ds.setLogDirectory("/Users/eslam.khoga/Desktop/logs");
        ds.setURL(CONNECTION_URL);
        ds.setCustomProperty("LogLevel", "6");
        ds.setCustomProperty("LogPath", "/Users/eslam.khoga/Desktop/logs");
        ds.setCustomProperty("EnablePaging", "1");
        ds.setCustomProperty("RowsPerPage", "5555");
        ds.setCustomProperty("TunableConsistency", "6"); // 1 - 10
        ds.setCustomProperty("cql.query.flags.page_size", "1");
        ds.setCustomProperty("cql.page_size", "1");
        ds.setCustomProperty("page_size", "1");
        ds.setCustomProperty("Flags", "0x23");
        connection = ds.getConnection();

        return connection;
    }
}