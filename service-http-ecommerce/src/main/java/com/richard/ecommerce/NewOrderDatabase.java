package com.richard.ecommerce;

import com.richard.LocalDatabase;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

public class NewOrderDatabase implements Closeable {

    private final LocalDatabase database;

    public NewOrderDatabase() throws SQLException {
        this.database = new LocalDatabase("orders_database");
        this.database.createIfNotExists("create table Orders (uuid varchar(200) primary key)");
    }

    public boolean sabeNew(Order order) throws SQLException {
        if (wasProcessed(order)) {
            return Boolean.FALSE;
        }
        this.database.update("insert into Orders (uuid) values (?)", order.getOrderId());
        return Boolean.TRUE;
    }

    private boolean wasProcessed(Order order) throws SQLException {
        var results = this.database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
        return results.next();
    }

    @Override
    public void close() throws IOException {
        try {
            this.database.close();
        } catch (SQLException ex) {
           throw new IOException(ex);
        }
    }
}
