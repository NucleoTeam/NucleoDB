package com.nucleocore.db.database.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;

public class TrieNodeTest {

    @Test
    public void verifyCorrectSize() {
        TrieNode node = new TrieNode();
        node.add("test", null);
        node.add("tax", null);
        node.add("text", null);
        assertTrue(node.search("test").size()==1);
        assertTrue(node.path.length==1);
        assertTrue(node.path[0].node.path.length==2);
    }

    @Test
    public void verifyCorrectSizeAfterDelete() {
        DataEntry de1 = new com.nucleocore.db.database.utils.Test(){{setKey("test");}};
        DataEntry de2 = new com.nucleocore.db.database.utils.Test(){{setKey("popcorn");}};
        DataEntry de3 = new com.nucleocore.db.database.utils.Test(){{setKey("corn");}};
        TrieNode node = new TrieNode();
        node.add("test", de1);
        node.add("tax", de2);
        node.add("text", de3);
        node.remove("tax", de2);
        assertTrue(node.search("test").size()==1);
        assertTrue(node.path.length==1);
        assertTrue(node.path[0].node.path.length==1);
    }

    @Test
    public void verifyCorrectSizeAfterRemoval() {
        DataEntry de1 = new com.nucleocore.db.database.utils.Test(){{setKey("test");}};
        DataEntry de2 = new com.nucleocore.db.database.utils.Test(){{setKey("popcorn");}};
        DataEntry de3 = new com.nucleocore.db.database.utils.Test(){{setKey("corn");}};
        DataEntry de4 = new com.nucleocore.db.database.utils.Test(){{setKey("icy");}};
        TrieNode node = new TrieNode();
        node.add("popcorn", de1);
        node.add("poppy", de2);
        node.add("pass", de3);
        node.add("plank", de4);
        node.remove("plank", de4);
        try {
            System.out.println(new ObjectMapper().writeValueAsString(node));
        }catch (Exception e){
            e.printStackTrace();
        }
        assertTrue(node.search("pass").size()==1);
        assertTrue(node.path.length==1);
        assertTrue(node.path[0].node.path.length==2);
    }

    @Test
    public void search() {
    }

    @Test
    public void deleteFromArray() {
    }

    @Test
    public void remove() {
    }
}