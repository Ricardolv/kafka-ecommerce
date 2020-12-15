package com.richard;

import java.sql.*;

public class LocalDatabase {

    private final Connection connection;

    public LocalDatabase(String name) throws SQLException {
        String url = "jdbc:sqlite:target/"+ name +".db";
        this.connection = DriverManager.getConnection(url);
    }

    public void createIfNotExists(String sql) {
        try {
            this.connection.createStatement().execute(sql);
        } catch (SQLException ex) {
            /// be careful, the sql could be wrong, be realy careful
            ex.printStackTrace();
        }
    }

    public void update(String statement, String ... params) throws SQLException {
        prepare(statement, params).execute();
    }

    public ResultSet query(String query, String ... params) throws SQLException {
        return prepare(query, params).executeQuery();
    }

    private PreparedStatement prepare(String statement, String[] params) throws SQLException {
        var prepareStatement = this.connection.prepareStatement(statement);
        for (int i = 0; i < params.length; i++) {
            prepareStatement.setString(i + 1, params[i]);
        }
        return prepareStatement;
    }

    public void close() throws SQLException {
        this.connection.close();
    }
}
