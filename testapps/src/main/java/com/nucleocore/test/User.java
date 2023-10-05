package com.nucleocore.test;


import com.nucleocore.library.database.tables.annotation.Relationship;
import com.nucleocore.library.database.tables.annotation.Relationships;
import com.nucleocore.library.database.tables.annotation.Table;
import com.nucleocore.test.sql.Anime;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


@Relationships({
    @Relationship(clazz = Anime.class, localKey = "name", remoteKey = "actors.name"),
    @Relationship(clazz = Anime.class, localKey = "name", remoteKey = "owner")
})
@Table("user")
public class User implements Serializable{
    private static final long serialVersionUID = 1;


    private String name;

    private List<UserNested> testingNested = new ArrayList<>();

    private String user;

    public User() {
        this.testingNested.add(new UserNested());
    }

    public User(User t) {
        this.name = t.name;
        this.user = t.user;
        this.testingNested.add(new UserNested());
    }


    public User(String name, String user) {
        this.name = name;
        this.user = user;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public List<UserNested> getTestingNested() {
        return testingNested;
    }

    public void setTestingNested(List<UserNested> testingNested) {
        this.testingNested = testingNested;
    }
}