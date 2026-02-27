package com.nucleodb.library.database.tables.connection;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Queues;
import com.nucleodb.library.database.tables.annotation.Conn;
import com.nucleodb.library.database.utils.StartupRun;
import com.nucleodb.library.event.ConnectionEventListener;
import com.nucleodb.library.mqs.config.MQSConfiguration;
import com.nucleodb.library.mqs.kafka.KafkaConfiguration;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

public class ConnectionConfig implements Serializable{
  private static final long serialVersionUID = 1;

  Instant readToTime = null;
  boolean write = true;
  boolean read = true;
  boolean loadSaved = true;
  boolean jsonExport = false;
  long exportInterval = 5000;
  boolean saveChanges = true;
  long saveInterval = 5000;
  String topic;
  String label;
  Class connectionClass;
  Class toTable;
  Class fromTable;
  ConnectionEventListener<Connection> eventListener = null;
  MQSConfiguration mqsConfiguration = new KafkaConfiguration();
  Map<String, Object> settingsMap = new TreeMap<>();
  String connectionFileName;
  ShardFilter shardFilter = new ShardFilter();
  @JsonIgnore
  private transient Queue<StartupRun> startupRuns = Queues.newLinkedBlockingDeque();

  public ConnectionConfig() {
  }

  public Instant getReadToTime() {
    return readToTime;
  }

  public void setReadToTime(Instant readToTime) {
    this.readToTime = readToTime;
  }

  public boolean isWrite() {
    return write;
  }

  public void setWrite(boolean write) {
    this.write = write;
  }

  public Queue<StartupRun> getStartupRuns() {
    return startupRuns;
  }

  public void setStartupRuns(Queue<StartupRun> startupRuns) {
    this.startupRuns = startupRuns;
  }

  public boolean isRead() {
    return read;
  }

  public void setRead(boolean read) {
    this.read = read;
  }

  public boolean isSaveChanges() {
    return saveChanges;
  }

  public void setSaveChanges(boolean saveChanges) {
    this.saveChanges = saveChanges;
  }

  public boolean isLoadSaved() {
    return loadSaved;
  }

  public void setLoadSaved(boolean loadSaved) {
    this.loadSaved = loadSaved;
  }

  public boolean isJsonExport() {
    return jsonExport;
  }

  public void setJsonExport(boolean jsonExport) {
    this.jsonExport = jsonExport;
  }

  public Class getConnectionClass() {
    return connectionClass;
  }

  public void setConnectionClass(Class connectionClass) {
    this.connectionClass = connectionClass;
  }

  public Class getToTable() {
    return toTable;
  }

  public void setToTable(Class toTable) {
    this.toTable = toTable;
  }

  public Class getFromTable() {
    return fromTable;
  }

  public void setFromTable(Class fromTable) {
    this.fromTable = fromTable;
  }

  public String getTopic() {

    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
    this.settingsMap.put("table", topic);
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
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

  public ConnectionEventListener<Connection> getEventListener() {
    return eventListener;
  }

  public void setEventListener(ConnectionEventListener<Connection> eventListener) {
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

  public String getConnectionFileName() {
    return connectionFileName;
  }

  public void setConnectionFileName(String connectionFileName) {
    this.connectionFileName = connectionFileName;
  }

  public ShardFilter getShardFilter() {
    return shardFilter;
  }

  public void setShardFilter(ShardFilter shardFilter) {
    this.shardFilter = shardFilter;
  }
}
