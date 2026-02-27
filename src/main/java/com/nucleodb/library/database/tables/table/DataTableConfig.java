package com.nucleodb.library.database.tables.table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Queues;
import com.nucleodb.library.database.index.IndexWrapper;
import com.nucleodb.library.database.utils.StartupRun;
import com.nucleodb.library.event.DataTableEventListener;
import com.nucleodb.library.mqs.config.MQSConfiguration;
import com.nucleodb.library.mqs.kafka.KafkaConfiguration;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;

public class DataTableConfig implements Serializable{
  private static final long serialVersionUID = 1;

  public static class IndexConfig implements Comparable {
    String name;
    Class<? extends IndexWrapper> indexType;

    public IndexConfig(String name, Class<? extends IndexWrapper> indexType) {
      this.name = name;
      this.indexType = indexType;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Class<? extends IndexWrapper> getIndexType() {
      return indexType;
    }

    public void setIndexType(Class<? extends IndexWrapper> indexType) {
      this.indexType = indexType;
    }

    @Override
    public int compareTo(@NotNull Object o) {
      if(o instanceof IndexConfig){
        return this.getName().compareTo(((IndexConfig) o).getName());
      }
      return 0;
    }
  }
  String table;
  @JsonIgnore
  transient Class clazz;
  long saveInterval = 5000;
  boolean saveChanges = true;
  boolean loadSave = true;
  boolean jsonExport = false;
  long exportInterval = 5000;
  Instant readToTime = null;
  Class dataEntryClass;
  boolean read = true;
  boolean write = true;
  DataTableEventListener<? extends DataEntry> eventListener = null;
  List<IndexConfig> indexes = new LinkedList<>();
  MQSConfiguration mqsConfiguration = new KafkaConfiguration();
  Map<String, Object> settingsMap = new TreeMap<>();
  String tableFileName;
  ShardFilter shardFilter = new ShardFilter();
  @JsonIgnore
  private transient Queue<StartupRun> startupRuns = Queues.newLinkedBlockingDeque();



  public DataTableConfig() {
  }


  public String getTable() {

    return table;
  }

  public Class getClazz() {
    return clazz;
  }

  public boolean isSaveChanges() {
    return saveChanges;
  }

  public boolean isLoadSave() {
    return loadSave;
  }

  public Instant getReadToTime() {
    return readToTime;
  }

  public boolean isRead() {
    return read;
  }

  public boolean isWrite() {
    return write;
  }

  public List<IndexConfig> getIndexes() {
    return indexes;
  }

  public void setTable(String table) {
    this.tableFileName = "./data/" + table + ".dat";
    this.table = table;
    this.settingsMap.put("table", table);
  }

  public void setTableFileName(String tableFileName) {
    this.tableFileName = tableFileName;
  }

  public void setClazz(Class clazz) {
    this.clazz = clazz;
  }

  public void setSaveChanges(boolean saveChanges) {
    this.saveChanges = saveChanges;
  }

  public void setLoadSave(boolean loadSave) {
    this.loadSave = loadSave;
  }

  public void setReadToTime(Instant readToTime) {
    this.readToTime = readToTime;
  }

  public void setRead(boolean read) {
    this.read = read;
  }

  public void setWrite(boolean write) {
    this.write = write;
  }

  public void setIndexes(List<IndexConfig> indexes) {
    this.indexes = indexes;
  }

  public Queue<StartupRun> getStartupRuns() {
    return startupRuns;
  }

  public void setStartupRuns(Queue<StartupRun> startupRuns) {
    this.startupRuns = startupRuns;
  }

  public void merge(DataTableConfig config) {
    this.indexes = config.indexes;
  }
  public String getTableFileName() {
    return tableFileName;
  }

  public boolean isJsonExport() {
    return jsonExport;
  }

  public void setJsonExport(boolean jsonExport) {
    this.jsonExport = jsonExport;
  }

  public Class getDataEntryClass() {
    return dataEntryClass;
  }

  public void setDataEntryClass(Class dataEntryClass) {
    this.dataEntryClass = dataEntryClass;
  }

  public MQSConfiguration getMqsConfiguration() {
    return mqsConfiguration;
  }

  public void setMqsConfiguration(MQSConfiguration mqsConfiguration) {
    this.mqsConfiguration = mqsConfiguration;
  }

  public Map<String, Object> getSettingsMap() {
    return settingsMap;
  }

  public void setSettingsMap(Map<String, Object> settingsMap) {
    this.settingsMap = settingsMap;
  }

  public DataTableEventListener<? extends DataEntry> getEventListener() {
    return eventListener;
  }

  public void setEventListener(DataTableEventListener<? extends DataEntry> eventListener) {
    this.eventListener = eventListener;
  }

  public long getSaveInterval() {
    return saveInterval;
  }

  public void setSaveInterval(long saveInterval) {
    this.saveInterval = saveInterval;
  }

  public long getExportInterval() {
    return exportInterval;
  }

  public void setExportInterval(long exportInterval) {
    this.exportInterval = exportInterval;
  }

  public ShardFilter getShardFilter() {
    return shardFilter;
  }

  public void setShardFilter(ShardFilter shardFilter) {
    this.shardFilter = shardFilter;
  }
}
