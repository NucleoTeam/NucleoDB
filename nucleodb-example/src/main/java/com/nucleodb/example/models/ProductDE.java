package com.nucleodb.example.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nucleodb.library.database.modifications.Create;
import com.nucleodb.library.database.tables.table.DataEntry;

public class ProductDE extends DataEntry<Product> {
    public ProductDE(Product obj) {
        super(obj);
    }

    public ProductDE(Create create) throws ClassNotFoundException, JsonProcessingException {
        super(create);
    }

    public ProductDE() {
    }

    public ProductDE(String key) {
        super(key);
    }
}
