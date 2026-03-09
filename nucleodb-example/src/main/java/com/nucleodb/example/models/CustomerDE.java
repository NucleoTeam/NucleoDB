package com.nucleodb.example.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nucleodb.library.database.modifications.Create;
import com.nucleodb.library.database.tables.table.DataEntry;

public class CustomerDE extends DataEntry<Customer> {
    public CustomerDE(Customer obj) {
        super(obj);
    }

    public CustomerDE(Create create) throws ClassNotFoundException, JsonProcessingException {
        super(create);
    }

    public CustomerDE() {
    }

    public CustomerDE(String key) {
        super(key);
    }
}
