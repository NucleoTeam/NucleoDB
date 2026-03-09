package com.nucleodb.example.models;

import com.nucleodb.library.database.tables.annotation.Conn;
import com.nucleodb.library.database.tables.connection.Connection;

import java.util.Map;

@Conn("PURCHASED")
public class PurchasedConnection extends Connection<CustomerDE, ProductDE> {
    public PurchasedConnection() {
    }

    public PurchasedConnection(CustomerDE from, ProductDE to) {
        super(from, to);
    }

    public PurchasedConnection(CustomerDE from, ProductDE to, Map<String, String> metadata) {
        super(from, to, metadata);
    }
}
