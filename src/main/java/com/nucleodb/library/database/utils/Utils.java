package com.nucleodb.library.database.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.lang.reflect.Field;
import java.util.Comparator;

public class Utils {
    public static long longRepresentation(String key){
        long x = 0;
        for (int i = 0; i < key.length(); i++) {
            long position = 100L*i;
            x += ((long)(key.charAt(i)-48))*((position>0)?position:1L);
        }
        return x;
    }
    public static int compare(Object a, Object b) {
        try {
            if (a == null && b == null)
                return 0;
            if (a == null)
                return -1;
            if (b == null)
                return 1;
            if (a.getClass() == String.class && b.getClass() == String.class) {
                return ((String) a).compareTo((String) b);
            } else if (a.getClass() == int.class && b.getClass() == int.class) {
                return Integer.valueOf((int) a).compareTo(Integer.valueOf((int) b));
            } else if (a.getClass() == long.class && b.getClass() == long.class) {
                return Long.valueOf((long) a).compareTo(Long.valueOf((long) b));
            } else if (a.getClass() == Long.class && b.getClass() == Long.class) {
                return ((Long) a).compareTo((Long) b);
            } else if (a.getClass() == Integer.class && b.getClass() == Integer.class) {
                return ((Integer) a).compareTo(((Integer) b));
            } else if (a.getClass() == boolean.class && b.getClass() == boolean.class) {
                return Boolean.valueOf((boolean) a).compareTo(Boolean.valueOf((boolean) b));
            } else {
                //System.out.println(a.getClass().getName());
                //System.out.println(b.getClass().getName());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
    public static boolean cast(Object a, Object b) {
        if (a == null && b == null)
            return true;
        if (a == null || b == null)
            return false;
        if (a.getClass() == String.class && b.getClass() == String.class) {
            return ((String) a).equals((String) b);
        } else if (a.getClass() == Integer.class && b.getClass() == Integer.class) {
            return ((Integer) a) == ((Integer) b);
        } else if (a.getClass() == Long.class && b.getClass() == Long.class) {
            return ((Long) a) == ((Long) b);
        } else if (a.getClass() == Boolean.class && b.getClass() == Boolean.class) {
            return ((Boolean) a) == ((Boolean) b);
        }
        return false;
    }

    private static ObjectMapper om = new ObjectMapper()
        .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .findAndRegisterModules();

    public static ObjectMapper getOm() {
        return om;
    }

    public static class SortByElement implements Comparator<Object> {
        Field f;

        public SortByElement(Field f) {
            this.f = f;
        }

        // Used for sorting in ascending order of
        // roll name
        public int compare(Object a, Object b) {
            try {
                return Utils.compare(f.get(a), f.get(b));
            } catch (Exception e) {
                e.printStackTrace();
            }
            return 0;
        }
    }
}
