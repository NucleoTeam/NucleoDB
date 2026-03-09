package com.nucleodb.example.models;

import com.nucleodb.library.database.index.TrieIndex;
import com.nucleodb.library.database.index.annotation.Index;
import com.nucleodb.library.database.tables.annotation.Table;

import java.io.Serializable;

@Table(tableName = "customer", dataEntryClass = CustomerDE.class)
public class Customer implements Serializable {
    private static final long serialVersionUID = 1L;

    @Index(type = TrieIndex.class)
    private String name;

    @Index
    private String email;

    public Customer() {
    }

    public Customer(String name, String email) {
        this.name = name;
        this.email = email;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    @Override
    public String toString() {
        return "Customer{name='" + name + "', email='" + email + "'}";
    }
}
