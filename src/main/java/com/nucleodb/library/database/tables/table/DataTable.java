package com.nucleodb.library.database.tables.table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.nucleodb.library.NucleoDB;
import com.nucleodb.library.database.modifications.Create;
import com.nucleodb.library.database.modifications.Delete;
import com.nucleodb.library.database.modifications.Modify;
import com.nucleodb.library.database.modifications.Update;
import com.nucleodb.library.database.utils.*;
import com.nucleodb.library.database.modifications.Modification;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryObjectException;
import com.nucleodb.library.database.index.IndexWrapper;
import com.nucleodb.library.database.utils.exceptions.InvalidIndexTypeException;
import com.nucleodb.library.event.DataTableEventListener;
import com.nucleodb.library.mqs.ConsumerHandler;
import com.nucleodb.library.mqs.ProducerHandler;
import com.github.fge.jsonpatch.JsonPatch;
import java.beans.IntrospectionException;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Logger;

public class DataTable<T extends DataEntry> implements Serializable{
  private static final long serialVersionUID = 1;
  @JsonIgnore
  private transient static Logger logger = Logger.getLogger(DataTable.class.getName());

  private Set<T> entries = new TreeSetExt<>();
  private transient DataTableConfig config;
  private String name;
  @JsonIgnore
  private transient Map<String, IndexWrapper<T>> indexes = new TreeMap<>();
  private Map<String, T> keyToEntry = new TreeMap<>();
  private Map<Integer, Long> partitionOffsets = new TreeMap<>();
  private long changed = new Date().getTime();
  private transient AtomicInteger leftInModQueue = new AtomicInteger(0);
  private Set<String> deletedEntries = new TreeSetExt<>();
  @JsonIgnore
  private transient NucleoDB nucleoDB;
  @JsonIgnore
  private transient ExportHandler exportHandler;
  @JsonIgnore
  private transient Queue<Object[]> indexQueue = Queues.newLinkedBlockingQueue();
  @JsonIgnore
  private transient ProducerHandler producer = null;

  @JsonIgnore
  private transient ConsumerHandler consumer = null;
  private transient AtomicInteger startupLoadCount = new AtomicInteger(0);
  private transient AtomicBoolean startupPhase = new AtomicBoolean(true);

  private transient Cache<String, Consumer<T>> changeListeners = CacheBuilder.newBuilder()
      .maximumSize(10000)
      .softValues()
      .expireAfterWrite(5, TimeUnit.SECONDS)
      .removalListener(e -> {
        if (e.getCause().name().equals("EXPIRED")) {
          //logger.info("EXPIRED" +e.getKey());
          System.exit(1);
          new Thread(() -> ((Consumer<T>) e.getValue()).accept(null)).start();
        }
      })
      .build();
  @JsonIgnore
  private Map<Modification, Set<Consumer<T>>> listeners = new TreeMap<>();
  @JsonIgnore
  private transient boolean unsavedIndexModifications = false;
  @JsonIgnore
  private transient int size = 0;
  @JsonIgnore
  private transient boolean buildIndex = true;
  @JsonIgnore
  private transient List<Field> fields;
  @JsonIgnore
  private transient boolean inStartup = true;
  private AtomicLong itemsToBeCleaned = new AtomicLong(0L);
  private transient Queue<ModificationQueueItem> modqueue = Queues.newLinkedBlockingQueue();

  public Object[] getIndex() {
    if (indexQueue.isEmpty())
      return null;
    return indexQueue.poll();
  }

  public void loadSavedData() {
    if (new File(config.getTableFileName()).exists()) {
      logger.info("reading " + config.getTableFileName());
      try {
        DataTable tmpTable = (DataTable) new ObjectFileReader().readObjectFromFile(config.getTableFileName());
        if (tmpTable.config != null)
          this.config.merge(tmpTable.config);
        this.changed = tmpTable.changed;
        this.entries = tmpTable.entries;
        this.partitionOffsets = tmpTable.partitionOffsets;
        this.keyToEntry = tmpTable.keyToEntry;
        this.entries.forEach(entry -> {
          for (IndexWrapper i : this.indexes.values()) {
            try {
              i.add(entry);
            } catch (JsonProcessingException ex) {
              throw new RuntimeException(ex);
            } catch (InvalidIndexTypeException ex) {
              throw new RuntimeException(ex);
            }
          }
          entry.dataTable = this;
          entry.setTableName(this.config.getTable());
        });
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public DataTable(DataTableConfig config) throws IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    this.config = config;

    config.getIndexes().forEach(i -> {
      try {
        IndexWrapper indexWrapper = i.getIndexType().getDeclaredConstructor(String.class).newInstance(i.getName());
        if (!this.indexes.containsKey(indexWrapper.getIndexedKey())) {
          this.indexes.put(indexWrapper.getIndexedKey(), indexWrapper);
        }
      } catch (InstantiationException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (InvocationTargetException e) {
        throw new RuntimeException(e);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    });

    if (config.isLoadSave()) {
      loadSavedData();
    }

    this.fields = new ArrayList<>(){{
      addAll(Arrays.asList(config.getClazz().getDeclaredFields()));
      addAll(Arrays.asList(config.getClazz().getSuperclass().getFields()));
    }};

    startRootConsumer();

    if (config.isRead()) {
      new Thread(new ModQueueHandler(this)).start();
    }

    if (config.isSaveChanges()) {
      new Thread(new SaveHandler(this)).start();
    }
    if (config.isJsonExport()) {
      exportHandler = new ExportHandler(this);
      new Thread(exportHandler).start();
    }
  }

  public void startRootConsumer() throws IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    this.consumer = this.getConfig()
        .getMqsConfiguration()
        .createConsumerHandler(this.config.getSettingsMap());
    this.consumer.setDatabase(this);
    this.consumer.start(36);
    this.config.getSettingsMap().put("consumerHandler", this.consumer);
    if (config.isWrite()) {
      producer = this.config
              .getMqsConfiguration()
              .createProducerHandler(this.config.getSettingsMap());
    }
  }

  @JsonIgnore
  private transient ExecutorService reloadExecutor = Executors.newSingleThreadExecutor();
  @JsonIgnore
  private transient Future<?> runningReload = null;
  public boolean reload(Consumer reloadComplete) {
    if(runningReload!=null && !runningReload.isDone()) return false;
    runningReload = reloadExecutor.submit(this.consumer.reload(reloadComplete));
    return true;
  }

  public void exportTo(DataTable tb) throws IncorrectDataEntryObjectException {
    for (T de : this.entries) {
      //Serializer.log("INSERTING " + de.getKey());
      try {
        tb.saveSync(de);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void flush() {
    try {
      entries.clear();
    } catch (Exception e) {
      //e.printStackTrace();
    }
    changeListeners.cleanUp();
    listeners = new HashMap<>();
    System.gc();
  }

  public Set<T> in(String key, List<Object> values, DataEntryProjection dataEntryProjection) {
    if (dataEntryProjection == null) {
      dataEntryProjection = new DataEntryProjection();
    }
    Set<T> tmp = new TreeSet<>();
    try {
      for (Object val : values) {
        Set<T> de = get(key, val, dataEntryProjection);
        if (de != null) {
          tmp.addAll(de);
        }
      }
    } catch (ClassCastException ex) {
      ex.printStackTrace();
    }
    return tmp;
  }

  public void startup() {
    inStartup = false;
    if (this.config.getStartupRuns() != null) {
      StartupRun startupRun = null;
      while((startupRun = this.config.getStartupRuns().poll())!=null) startupRun.run(this);
    }
  }

  private long counter = 0;
  private long lastReq = 0;

  public Set<T> handleIndexOperation(Object obj, DataEntryProjection dataEntryProjection, Function<Object, Set<T>> func) {
    if (dataEntryProjection == null) {
      dataEntryProjection = new DataEntryProjection();
    }
    Set<T> apply = func.apply(obj);
    if(apply!=null) {
      return dataEntryProjection.process(apply.stream(), this.getConfig().getDataEntryClass());
    }
    return Sets.newTreeSet();
  }

  public Set<T> handleIndexStringOperation(String obj, DataEntryProjection dataEntryProjection, Function<String, Set<T>> func) {
    if (dataEntryProjection == null) {
      dataEntryProjection = new DataEntryProjection();
    }
    Set<T> apply = func.apply(obj);
    if(apply!=null) {
      return dataEntryProjection.process(apply.stream(), this.getConfig().getDataEntryClass());
    }
    return Sets.newTreeSet();
  }

  public Set<T> search(String key, Object searchObject, DataEntryProjection dataEntryProjection) {
    IndexWrapper<T> TIndexWrapper = this.indexes.get(key);
    return handleIndexOperation(searchObject, dataEntryProjection, TIndexWrapper::contains);
  }

  public Set<T> startsWith(String key, String str, DataEntryProjection dataEntryProjection) throws InvalidIndexTypeException {
    IndexWrapper<T> TIndexWrapper = this.indexes.get(key);
    return handleIndexStringOperation(str, dataEntryProjection, TIndexWrapper::startsWith);
  }


  public Set<T> endsWith(String key, String str, DataEntryProjection dataEntryProjection) throws InvalidIndexTypeException {
    IndexWrapper<T> TIndexWrapper = this.indexes.get(key);
    return handleIndexStringOperation(str, dataEntryProjection, TIndexWrapper::endsWith);
  }

  public Set<T> greaterThan(String key, Object obj, DataEntryProjection dataEntryProjection) throws InvalidIndexTypeException {
    IndexWrapper<T> TIndexWrapper = this.indexes.get(key);
    return handleIndexOperation(obj, dataEntryProjection, TIndexWrapper::greaterThan);
  }

  public Set<T> greaterThanEqual(String key, Object obj, DataEntryProjection dataEntryProjection) throws InvalidIndexTypeException {
    IndexWrapper<T> TIndexWrapper = this.indexes.get(key);
    return handleIndexOperation(obj, dataEntryProjection, TIndexWrapper::greaterThanEqual);
  }

  public Set<T> lessThan(String key, Object obj, DataEntryProjection dataEntryProjection) throws InvalidIndexTypeException {
    IndexWrapper<T> TIndexWrapper = this.indexes.get(key);
    return handleIndexOperation(obj, dataEntryProjection, TIndexWrapper::lessThan);
  }

  public Set<T> lessThanEqual(String key, Object obj, DataEntryProjection dataEntryProjection) throws InvalidIndexTypeException {
    IndexWrapper<T> TIndexWrapper = this.indexes.get(key);
    return handleIndexOperation(obj, dataEntryProjection, TIndexWrapper::lessThanEqual);
  }

  public Set<T> get(String key, Object value) {
    return get(key, value, null);
  }

  public Set<T> get(String key, Object value, DataEntryProjection dataEntryProjection) {
    if (dataEntryProjection == null) {
      dataEntryProjection = new DataEntryProjection();
    }
    if (key.equals("id")) {
      if (value != null && this.keyToEntry.size() > 0) {
        T T = this.keyToEntry.get(value);
        if (T != null) {
          Set<T> entrySet = new TreeSet(Arrays.asList(T));
          return dataEntryProjection.process(entrySet.stream(), this.getConfig().getDataEntryClass());
        }
      }
      return new TreeSet<>();
    }
    IndexWrapper<T> TIndexWrapper = this.indexes.get(key);
    return handleIndexOperation(value, dataEntryProjection, TIndexWrapper::get);
  }

  public Set<T> getNotEqual(String key, Object value, DataEntryProjection dataEntryProjection) {
    if (dataEntryProjection == null) {
      dataEntryProjection = new DataEntryProjection();
    }
    try {
      Set<T> foundEntries;
      if (key.equals("id")) {
        foundEntries = new TreeSet<>(Arrays.asList(this.keyToEntry.get(value)));
      } else {
        foundEntries = this.indexes.get(key).get(value);
      }
      Set<T> negation = new TreeSet<>(entries);
      negation.removeAll(foundEntries);
      if (entries != null) {
        return dataEntryProjection.process(negation.stream(), this.getConfig().getDataEntryClass());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return new TreeSet<>();
  }


  public T searchOne(String key, Object obj, DataEntryProjection dataEntryProjection) {
    if (dataEntryProjection == null) {
      dataEntryProjection = new DataEntryProjection();
    }
    Set<T> entries = search(key, obj, dataEntryProjection);
    if (entries != null && entries.size() > 0) {
      Set<T> d = dataEntryProjection.process(entries.stream(), this.getConfig().getDataEntryClass());
      if (d.size()>0)
        return d.stream().findFirst().get();
    }
    return null;
  }

  public int size() {
    return size;
  }

  public boolean deleteSync(T obj) throws InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    boolean v = deleteInternalConsumer(obj, (de) -> {
      countDownLatch.countDown();
    });
    countDownLatch.await();
    return v;
  }

  public boolean deleteAsync(T obj, Consumer<T> consumer) {
    return deleteInternalConsumer(obj, consumer);
  }

  public boolean delete(T entry) {
    return deleteInternalConsumer(entry, null);
  }

  private boolean deleteInternalConsumer(T entry, Consumer<T> consumer) {
    if (!this.config.isWrite()) {
      if (consumer != null) consumer.accept(null);
      return false;
    }
    String changeUUID = UUID.randomUUID().toString();
    entry.versionIncrease();
    Delete delete = new Delete(changeUUID, entry);
    if (consumer != null) {
      changeListeners.put(changeUUID, consumer);
    }
    producer.push(delete.getKey(), delete.getVersion(), delete, null);
    return true;
  }


  public boolean saveSync(T entry) throws InterruptedException, IncorrectDataEntryObjectException {
    if (entry.getClass() != getConfig().getDataEntryClass()) {
      throw new IncorrectDataEntryObjectException("Entry " + entry.getKey() + " using incorrect data entry class for this table.");
    }
    CountDownLatch countDownLatch = new CountDownLatch(1);
    boolean v = saveInternalConsumer(entry, (de) -> {
      countDownLatch.countDown();
    });
    countDownLatch.await();
    return v;
  }

  public boolean saveAndForget(T entry) throws IncorrectDataEntryObjectException {
    if (entry.getClass() != getConfig().getDataEntryClass()) {
      throw new IncorrectDataEntryObjectException("Entry " + entry.getKey() + " using incorrect data entry class for this table.");
    }
    return saveInternalConsumer(entry, null);
  }

  public boolean saveAsync(T entry, Consumer<T> consumer) throws IncorrectDataEntryObjectException {
    if (entry.getClass() != getConfig().getDataEntryClass()) {
      throw new IncorrectDataEntryObjectException("Entry " + entry.getKey() + " using incorrect data entry class for this table.");
    }
    return saveInternalConsumer(entry, consumer);
  }

  public boolean saveSync(Object data) throws InterruptedException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IncorrectDataEntryObjectException {
    return saveSync((T) this.getConfig().getDataEntryClass().getDeclaredConstructor(data.getClass()).newInstance(data));
  }

  public boolean saveAndForget(Object data) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IncorrectDataEntryObjectException {
    return saveAndForget((T) this.getConfig().getDataEntryClass().getDeclaredConstructor(data.getClass()).newInstance(data));
  }

  public boolean saveAsync(Object data, Consumer<T> consumer) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IncorrectDataEntryObjectException {
    return saveAsync((T) this.getConfig().getDataEntryClass().getDeclaredConstructor(data.getClass()).newInstance(data), consumer);
  }

  private boolean saveInternalConsumer(T entry, Consumer<T> consumer) {
    if (!this.config.isWrite()) {
      if (consumer != null) consumer.accept(null);
      return false;
    }
    String changeUUID = UUID.randomUUID().toString();
    if (consumer != null) {
      changeListeners.put(changeUUID, consumer);
    }
    if (saveInternal(entry, changeUUID)) {
      return true;
    }
    return false;
  }

  // convert object to jsonnode without changing types
  JsonNode fromObject(Object o){
    try {
      return Serializer.getObjectMapper().getOmNonType().readTree(
          Serializer.getObjectMapper().getOm().writeValueAsString(o)
      );
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
  Object fromJsonNode(JsonNode o, Class type){
    try {
      return Serializer.getObjectMapper().getOm().readValue(
          Serializer.getObjectMapper().getOmNonType().writeValueAsString(o),
          type
      );
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean saveInternal(T entry, String changeUUID) {
    if (!entries.contains(entry)) {
      try {
        Create createEntry = new Create(changeUUID, entry);
        producer.push(createEntry.key, createEntry.version, createEntry, null);
        return true;
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      entry.versionIncrease();
      List<JsonOperations> changes = null;
      Set<T> TSet = get("id", entry.getKey());
      if(TSet==null || TSet.isEmpty()){
        try {
          consumerResponse(null, changeUUID);
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
        return false;
      }
      T T = TSet.iterator().next();
      JsonPatch patch = JsonDiff.asJsonPatch(fromObject(T.getData()), fromObject(entry.getData()));
      try {
        String json = Serializer.getObjectMapper().getOmNonType().writeValueAsString(patch);
        changes = Serializer.getObjectMapper().getOmNonType().readValue(json, List.class);

        if (changes != null && changes.size() > 0) {
          Update updateEntry = new Update(changeUUID, entry, json);
          producer.push(updateEntry.getKey(), updateEntry.getVersion(), updateEntry, null);
          return true;
        }else{
          try {
            T.setRequest(entry.getRequest());
            consumerResponse(T, changeUUID);
          } catch (ExecutionException e) {
            throw new RuntimeException(e);
          }
        }
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      }

    }
    return false;
  }

  private void itemProcessed() {
    if (this.startupPhase.get()) {
      int left = this.startupLoadCount.decrementAndGet();
      if (left != 0 && left % 10000 == 0)
        logger.info("startup " + this.getConfig().getTable() + " items waiting to process: " + left);
      if (!this.getConsumer().getStartupPhaseConsume().get() && left <= 0) {
        this.startupPhase.set(false);
        System.gc();
        new Thread(() -> this.startup()).start();
      }
    }
  }

  private void triggerEvent(Modify modify, T T) {
    DataTableEventListener eventListener = config.getEventListener();
    if (eventListener != null) {
      if (modify instanceof Create) {
        new Thread(() -> eventListener.create((Create) modify, T)).start();
      } else if (modify instanceof Delete) {
        new Thread(() -> eventListener.delete((Delete) modify, T)).start();
      } else if (modify instanceof Update) {
        new Thread(() -> eventListener.update((Update) modify, T)).start();
      }
    }
  }

  private void itemRequeue() {
    if (this.startupPhase.get()) this.startupLoadCount.incrementAndGet();
  }

  public void modify(Modification mod, Object modification) throws ExecutionException {
    switch (mod) {
      case CREATE:
        Create c = (Create) modification;
        if(!config.getShardFilter().create(c)){
          consumerResponse(null, c.getChangeUUID());
          return;
        }
        //if(!startupPhase.get()) logger.info("Create statement called");
        if (c != null) {
          if (deletedEntries.contains(c.getKey())) {
            consumerResponse(null, c.getChangeUUID());
            return;
          }
          try {
            itemProcessed();
            if (this.config.getReadToTime() != null && c.getTime().isAfter(this.config.getReadToTime())) {
              consumerResponse(null, c.getChangeUUID());
              return;
            }

            if (keyToEntry.containsKey(c.getKey())) {
              //logger.info("Ignore already saved change.");
              //System.exit(1);
              return; // ignore this create
            }

            T T = (T) this.getConfig().getDataEntryClass().getDeclaredConstructor(Create.class).newInstance(c);

            T.setTableName(this.config.getTable());
            T.dataTable = this;

            synchronized (entries) {
              entries.add(T);
              keyToEntry.put(T.getKey(), T);
              for (IndexWrapper i : this.indexes.values()) {
                try {
                  i.add(T);
                } catch (JsonProcessingException e) {
                  throw new RuntimeException(e);
                }
              }
            }
            this.changed = new Date().getTime();
            size++;
            consumerResponse(T, c.getChangeUUID());
            fireListeners(Modification.CREATE, T);
            triggerEvent(c, T);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        break;
      case DELETE:
        Delete d = (Delete) modification;

        //if(!startupPhase.get()) logger.info("Delete statement called");
        if (d != null) {
          try {
            itemProcessed();
            T de = keyToEntry.get(d.getKey());
            if(de!=null && !config.getShardFilter().delete(d, de)){
              consumerResponse(null, d.getChangeUUID());
              return;
            }
            if (this.config.getReadToTime() != null && d.getTime().isAfter(this.config.getReadToTime())) {
              consumerResponse(null, d.getChangeUUID());
              return;
            }


            if (de != null) {
              if (de.getVersion() >= d.getVersion()) {
                logger.info("Ignore already saved change.");
                consumerResponse(de, d.getChangeUUID());
                fireListeners(Modification.DELETE, de);
                return; // ignore change
              }
              if (de.getVersion() + 1 != d.getVersion()) {
                logger.info("Version not ready!");
                itemRequeue();
                modqueue.add(new ModificationQueueItem(mod, modification));
                leftInModQueue.incrementAndGet();
                synchronized (modqueue) {
                  modqueue.notifyAll();
                }
                Serializer.log("ADDED TO MOD QUEUE");
              } else {
                deletedEntries.add(de.getKey());
                synchronized (entries) {
                  entries.remove(de);
                  keyToEntry.remove(de.getKey());
                  this.indexes.values().forEach(i -> {
                    try {
                      i.delete(de);
                    } catch (InvalidIndexTypeException e) {
                      throw new RuntimeException(e);
                    }
                  });
                }
                this.changed = new Date().getTime();
                size--;
                de.setRequest(d.getRequest());
                consumerResponse(de, d.getChangeUUID());
                fireListeners(Modification.DELETE, de);
                triggerEvent(d, de);
                long items = itemsToBeCleaned.incrementAndGet();
                if (!startupPhase.get() && items > 100) {
                  itemsToBeCleaned.set(0L);
                  System.gc();
                }
              }
            } else {
              if (deletedEntries.contains(d.getKey())) {
                //logger.info("ERROR already deleted: "+d.getKey()+" table: "+this.config.getTable());
                //System.exit(1);
                consumerResponse(de, d.getChangeUUID());
                fireListeners(Modification.DELETE, de);
                return;
              } else {
                itemRequeue();
                modqueue.add(new ModificationQueueItem(mod, modification));
                leftInModQueue.incrementAndGet();
                synchronized (modqueue) {
                  modqueue.notifyAll();
                }
              }
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        break;
      case UPDATE:
        Update u = (Update) modification;
        //if(!startupPhase.get()) logger.info("Update statement called");
        if (u != null) {
          try {
            itemProcessed();
            if (this.config.getReadToTime() != null && u.getTime().isAfter(this.config.getReadToTime())) {
              //logger.info("Update after target db date");
              consumerResponse(null, u.getChangeUUID());
              return;
            }
            T de = keyToEntry.get(u.getKey());
            if (de != null) {
              if (de.getVersion() >= u.getVersion()) {
                logger.info("Ignore already saved change. " + de.getKey()+" table: "+ getConfig().table);
                consumerResponse(null, u.getChangeUUID());
                return; // ignore change
              }
              if (de.getVersion() + 1 != u.getVersion()) {
                //logger.info("Version not ready!" + de.getKey()+" table: "+ getConfig().table);
                itemRequeue();
                modqueue.add(new ModificationQueueItem(mod, modification));
                leftInModQueue.incrementAndGet();
                synchronized (modqueue) {
                  modqueue.notifyAll();
                }
              } else {
                de.setData(fromJsonNode(u.getChangesPatch().apply(fromObject(de.getData())), de.getData().getClass()));
                de.setVersion(u.getVersion());
                de.setModified(u.getTime());
                de.setRequest(u.getRequest());
                u.getOperations().forEach(op -> {
                  switch (op.getOp()) {
                    case "replace":
                    case "add":
                    case "copy":
                      try {
                        synchronized (entries) {
                          IndexWrapper indexWrapper = this.indexes.get(op.getPath());
                          if (indexWrapper != null) {
                            indexWrapper.modify(de);
                          }
                        }
                      } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                      } catch (InvalidIndexTypeException e) {
                        throw new RuntimeException(e);
                      }
                      break;
                    case "move":
                      break;
                    case "remove":
                      synchronized (entries) {
                        IndexWrapper indexWrapper = this.indexes.get(op.getPath());
                        if (indexWrapper != null) {
                          try {
                            indexWrapper.delete(de);
                          } catch (InvalidIndexTypeException e) {
                            throw new RuntimeException(e);
                          }
                        }
                      }
                      break;
                  }
                });
                this.changed = new Date().getTime();
                consumerResponse(de, u.getChangeUUID());
                fireListeners(Modification.UPDATE, de);
                triggerEvent(u, de);
                long items = itemsToBeCleaned.incrementAndGet();
                if (!startupPhase.get() && items > 100) {
                  itemsToBeCleaned.set(0L);
                  System.gc();
                }
              }
            } else {
              if (!deletedEntries.contains(u.getKey())) {
                itemRequeue();
                modqueue.add(new ModificationQueueItem(mod, modification));
                leftInModQueue.incrementAndGet();
                synchronized (modqueue) {
                  modqueue.notifyAll();
                }
              }else{
                consumerResponse(null, u.getChangeUUID());
              }
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        break;
    }
  }

  private void consumerResponse(T T, String changeUUID) throws ExecutionException {
    try {
      if(T!=null){
        getNucleoDB().getLockManager().releaseLock(this.config.getTable(), T.getKey(), T.getRequest());
      }
      if (changeUUID != null) {

        Consumer<T> TConsumer = changeListeners.getIfPresent(changeUUID);
        if (TConsumer != null) {
          new Thread(() -> TConsumer.accept(T)).start();
          changeListeners.invalidate(changeUUID);
        }
      }
    } catch (CacheLoader.InvalidCacheLoadException e) {
    }
    this.changed = new Date().getTime();
  }

  public void fireListeners(Modification m, T data) {
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

  public void multiImport(T newEntry) throws InterruptedException, IncorrectDataEntryObjectException {
    this.saveSync(newEntry);
  }


  @JsonIgnore
  transient List<Thread> threads = new ArrayList<>();


  public void addListener(Modification m, Consumer<T> method) {
    if (!listeners.containsKey(m)) {
      listeners.put(m, new HashSet<>());
    }
    listeners.get(m).add(method);
  }

  public Set<T> getEntries() {
    return entries;
  }

  public void setEntries(Set<T> entries) {
    this.entries = entries;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public boolean isBuildIndex() {
    return buildIndex;
  }

  public void setBuildIndex(boolean buildIndex) {
    this.buildIndex = buildIndex;
  }

  public boolean isUnsavedIndexModifications() {
    return unsavedIndexModifications;
  }

  public void setUnsavedIndexModifications(boolean unsavedIndexModifications) {
    this.unsavedIndexModifications = unsavedIndexModifications;
  }

  public Map<String, IndexWrapper<T>> getIndexes() {
    return indexes;
  }

  public void setIndexes(Map<String, IndexWrapper<T>> indexes) {
    this.indexes = indexes;
  }

  public Map<String, T> getKeyToEntry() {
    return keyToEntry;
  }

  public void setKeyToEntry(Map<String, T> keyToEntry) {
    this.keyToEntry = keyToEntry;
  }

  public long getChanged() {
    return changed;
  }

  public void setChanged(long changed) {
    this.changed = changed;
  }

  public Queue<Object[]> getIndexQueue() {
    return indexQueue;
  }

  public void setIndexQueue(Queue<Object[]> indexQueue) {
    this.indexQueue = indexQueue;
  }

  public ProducerHandler getProducer() {
    return producer;
  }

  public void setProducer(ProducerHandler producer) {
    this.producer = producer;
  }

  public ConsumerHandler getConsumer() {
    return consumer;
  }

  public void setConsumer(ConsumerHandler consumer) {
    this.consumer = consumer;
  }

  public Cache<String, Consumer<T>> getChangeListeners() {
    return changeListeners;
  }

  public void setChangeListeners(Cache<String, Consumer<T>> changeListeners) {
    this.changeListeners = changeListeners;
  }

  public Map<Modification, Set<Consumer<T>>> getListeners() {
    return listeners;
  }

  public void setListeners(Map<Modification, Set<Consumer<T>>> listeners) {
    this.listeners = listeners;
  }

  public List<Field> getFields() {
    return fields;
  }

  public void setFields(List<Field> fields) {
    this.fields = fields;
  }

  public boolean isInStartup() {
    return inStartup;
  }

  public void setInStartup(boolean inStartup) {
    this.inStartup = inStartup;
  }

  public long getCounter() {
    return counter;
  }

  public void setCounter(long counter) {
    this.counter = counter;
  }

  public long getLastReq() {
    return lastReq;
  }

  public void setLastReq(long lastReq) {
    this.lastReq = lastReq;
  }

  public Queue<ModificationQueueItem> getModqueue() {
    return modqueue;
  }

  public void setModqueue(Queue<ModificationQueueItem> modqueue) {
    this.modqueue = modqueue;
  }

  public List<Thread> getThreads() {
    return threads;
  }

  public void setThreads(List<Thread> threads) {
    this.threads = threads;
  }

  public Map<Integer, Long> getPartitionOffsets() {
    return partitionOffsets;
  }

  public void setPartitionOffsets(Map<Integer, Long> partitionOffsets) {
    this.partitionOffsets = partitionOffsets;
  }

  public DataTableConfig getConfig() {
    return config;
  }

  public void setConfig(DataTableConfig config) {
    this.config = config;
  }

  public AtomicInteger getLeftInModQueue() {
    return leftInModQueue;
  }

  public void setLeftInModQueue(AtomicInteger leftInModQueue) {
    this.leftInModQueue = leftInModQueue;
  }

  public AtomicInteger getStartupLoadCount() {
    return startupLoadCount;
  }

  public void setStartupLoadCount(AtomicInteger startupLoadCount) {
    this.startupLoadCount = startupLoadCount;
  }

  public AtomicBoolean getStartupPhase() {
    return startupPhase;
  }

  public void setStartupPhase(AtomicBoolean startupPhase) {
    this.startupPhase = startupPhase;
  }

  public NucleoDB getNucleoDB() {
    return nucleoDB;
  }

  public void setNucleoDB(NucleoDB nucleoDB) {
    this.nucleoDB = nucleoDB;
  }

  public ExportHandler getExportHandler() {
    return exportHandler;
  }

  public void setExportHandler(ExportHandler exportHandler) {
    this.exportHandler = exportHandler;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
