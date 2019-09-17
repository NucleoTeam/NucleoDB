package com.nucleocore.db.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayTable;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.nucleocore.db.database.modifications.Create;
import com.nucleocore.db.database.modifications.Delete;
import com.nucleocore.db.database.modifications.Update;
import com.nucleocore.db.database.utils.*;
import com.nucleocore.db.kafka.ConsumerHandler;
import com.nucleocore.db.kafka.ProducerHandler;

import javax.swing.text.html.Option;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LargeDataTable implements TableTemplate {

    private ProducerHandler producer = null;
    private ConsumerHandler consumer = null;

    private List<DataEntry> entries = Lists.newArrayList();
    private TreeMap<String, List<DataEntry>> sortedIndex = new TreeMap<>();

    private HashMap<String, Consumer<DataEntry>> consumers = new HashMap<>();

    private HashMap<Modification, Set<Consumer<DataEntry>>> listeners = new HashMap<>();

    private ObjectMapper om = new ObjectMapper();

    private int size = 0;

    private Stack<DataEntry> importList = new Stack<>();

    public LargeDataTable(String bootstrap, String table) {
        if (bootstrap != null) {
            producer = new ProducerHandler(bootstrap, table);
            consumer = new ConsumerHandler(bootstrap, UUID.randomUUID().toString(), this, table);
        }
    }

    public void flush() {
        try {
            synchronized (entries) {
                entries.clear();
            }
        }catch (Exception e){
            //e.printStackTrace();
        }
        consumers = new HashMap<>();
        listeners = new HashMap<>();
        System.gc();
    }

    class SortByElement implements Comparator<Object>
    {
        Field f;
        public SortByElement(Field f){
            this.f = f;
        }
        // Used for sorting in ascending order of
        // roll name
        public int compare(Object a, Object b)
        {
            try {
                return LargeDataTable.this.compare(f.get(a), f.get(b));
            }catch (Exception e){
                e.printStackTrace();
            }
            return 0;
        }
    }

    public void updateIndex(Class clazz){
        for (Field f: clazz.getDeclaredFields()) {
            String fieldName = f.getName();
            System.out.println(fieldName+" sorting");
            if (!sortedIndex.containsKey(fieldName)) {
                sortedIndex.put(fieldName, Lists.newArrayList());
                sortedIndex.get(fieldName).addAll(entries);
            }
            Collections.sort(sortedIndex.get(fieldName), new SortByElement(f));
        }
    }


    public Set<DataEntry> search(String name, Object obj, Class clazz) {
        try {
            Field f = clazz.getField(name);
            if (sortedIndex.containsKey(name)) {
                List<DataEntry> deList = sortedIndex.get(name);
                System.out.println(new ObjectMapper().writeValueAsString(deList));
                int end = deList.size();
                int pos = (int)Math.floor(end/2);
                int start=0;
                Set<DataEntry> set = Sets.newHashSet();
                while(start<pos){
                    int val = compare(obj, f.get(deList.get(pos)));
                    System.out.println("direction: "+val+" start: "+start +" end: "+end+" pos:"+pos);
                    System.out.println(f.get(deList.get(pos)));
                    System.out.println(obj);
                    if(val>0){
                        start = pos;
                        pos = start+(int)Math.floor((end-pos)/2);
                    }else if(val<0){
                        end = pos;
                        pos = (int)Math.floor((pos-start)/2);
                    }else if(val==0){
                        set.add(deList.get(pos));
                        break;
                    }
                }
                return set;
            }
            return entries.parallelStream().filter(i -> {
                try {
                    return cast(f.get(i),obj);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("ERRR 3");
                    System.exit(-1);
                }
                return false;
            }).collect(Collectors.toSet());
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("ERRR 4");
            System.exit(-1);
            return null;
        }
    }

    public int size() {
        return size;
    }


    public boolean cast(Object a, Object b) {
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
    public <T> int compare(T a, T b) {
        try {
            if(a==null || b==null)
                return -1;
            if (a.getClass() == String.class && b.getClass() == String.class) {
                return ((String) a).compareTo((String) b);
            } else if (a.getClass() == int.class && b.getClass() == int.class) {
                return Integer.valueOf((int) a).compareTo(Integer.valueOf((int) b));
            } else if (a.getClass() == long.class && b.getClass() == long.class) {
                return Long.valueOf((long) a).compareTo(Long.valueOf((long) b));
            } else if (a.getClass() == boolean.class && b.getClass() == boolean.class) {
                return Boolean.valueOf((boolean) a).compareTo(Boolean.valueOf((boolean) b));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }

    public synchronized boolean save(DataEntry oldEntry, DataEntry newEntry) {
        return save(oldEntry, newEntry, null);
    }

    public synchronized boolean save(DataEntry oldEntry, DataEntry newEntry, Consumer<DataEntry> consumer) {
        if (oldEntry == null && newEntry != null) {
            try {
                Create createEntry = new Create(newEntry.getKey(), newEntry);
                if (consumer != null) {
                    consumers.put(newEntry.getKey(), consumer);
                }
                if (producer != null)
                    producer.save(createEntry);
                else
                    modify(Modification.CREATE, createEntry);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (newEntry == null && oldEntry != null) {
            Delete deleteEntry = new Delete(oldEntry.getKey());
            if (consumer != null) {
                consumers.put(oldEntry.getKey(), consumer);
            }
            if (producer != null)
                producer.save(deleteEntry);
            else
                modify(Modification.DELETE, deleteEntry);
        } else if (newEntry != null && oldEntry != null) {
            Update updateEntry = new Update();
            try {
                updateEntry.setKey(newEntry.getKey());
                if (consumer != null) {
                    consumers.put(newEntry.getKey(), consumer);
                }
                boolean changed = false;
                updateEntry.setMasterClass(newEntry.getClass().getName());
                for (Field f : newEntry.getClass().getDeclaredFields()) {
                    if (!cast(f.get(newEntry), f.get(oldEntry))) {
                        if (f.get(newEntry) != null) {
                            updateEntry.getChange().put(f.getName(), f.get(newEntry));
                            changed = true;
                        }
                    }
                }
                if (changed) {
                    System.out.println("Changed");
                    if (producer != null)
                        producer.save(updateEntry);
                    else
                        modify(Modification.UPDATE, updateEntry);
                    return true;
                }
                System.out.println("Nothing changed");
                return false;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        } else
            return false;
        return true;
    }

    public synchronized void modify(Modification mod, Object modification) {
        switch (mod) {
            case CREATE:
                Create c = (Create) modification;
                //System.out.println("Create statement called");
                if (c != null) {
                    try {
                        synchronized (entries) {
                            entries.add(c.getValue());
                        }
                        size++;
                        if (consumers.containsKey(c.getValue().getKey())) {
                            consumers.remove(c.getValue().getKey()).accept(c.getValue());
                        }
                        fireListeners(Modification.CREATE, c.getValue());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                break;
            case DELETE:
                Delete d = (Delete) modification;
                //System.out.println("Delete statement called");
                if (d != null) {
                    synchronized (entries) {
                        Optional<DataEntry> entry = entries.stream().filter(x->x.getKey().equals(d.getKey())).findFirst();
                        entries.removeIf(x->x.getKey().equals(d.getKey()));
                        size--;
                        if(entry.isPresent()) {
                            DataEntry de = entry.get();
                            if (consumers.containsKey(d.getKey())) {
                                consumers.remove(d.getKey()).accept(de);
                            }
                            fireListeners(Modification.DELETE, de);
                        }
                    }
                }
                break;
            case UPDATE:
                Update u = (Update) modification;
                //System.out.println("Update statement called");

                if (u != null) {
                    try {
                        Class clazz = Class.forName(u.getMasterClass());
                        Optional<DataEntry> obj = entries.stream().filter(x->x.getKey().equals(u.getKey())).findFirst();
                        if(obj.isPresent()) {
                            DataEntry de = obj.get();
                            if (consumers.containsKey(de.getKey())) {
                                consumers.remove(de.getKey()).accept(de);
                            }
                            u.getChange().forEach((String key, Object val) -> {
                                try {
                                    clazz.getDeclaredField(key).set(obj, val);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            });
                            fireListeners(Modification.UPDATE, de);
                        }
                    } catch (Exception e) {

                    }
                }
                break;
        }
    }

    public void fireListeners(Modification m, DataEntry data) {
        if (listeners.containsKey(m)) {
            listeners.get(m).forEach(method -> {
                try {
                    method.accept(data);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public void multiImport(DataEntry newEntry) {
        synchronized (importList) {
            this.save(null, newEntry);
        }
    }

    List<Thread> threads = new ArrayList<>();

    public void startImportThreads() {
        for (int i = 0; i < 1; i++) {
            Thread t = new Thread(() -> {
                while (!Thread.interrupted()) {
                    DataEntry de = null;
                    synchronized (importList) {
                        if (importList.size() > 0)
                            de = importList.pop();
                    }
                    if (de != null) {
                        this.save(null, de);
                    }
                }
            });
            t.start();
            threads.add(t);
        }
    }

    public void stopImportThreads() {
        while (threads.size() > 0) {
            threads.remove(0).interrupt();
        }
        threads.clear();
    }

    public void addListener(Modification m, Consumer<DataEntry> method) {
        if (!listeners.containsKey(m)) {
            listeners.put(m, new HashSet<>());
        }
        listeners.get(m).add(method);
    }

}