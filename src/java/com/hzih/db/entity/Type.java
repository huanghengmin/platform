package com.hzih.db.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 15-11-9.
 */
public class Type {
    public static String s_app_db = "db";
    public static String s_app_sipproxy = "sip";
    public static String s_app_file = "file";
    public static String s_app_proxy = "proxy";
    private String appName;
    private String appType;
    private String appDesc;
    private String sourceDB;
    private String sourceTempTable;
    private String targetDB;
    private String targetTempTable;
    private int maxRecords;
    private int interval;
    private boolean isActive;

    private List<Table> tableList = new ArrayList<Table>();
    private List<Sequence> sequenceList = new ArrayList<Sequence>();

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getAppType() {
        return appType;
    }

    public void setAppType(String appType) {
        this.appType = appType;
    }

    public String getAppDesc() {
        return appDesc;
    }

    public void setAppDesc(String appDesc) {
        this.appDesc = appDesc;
    }

    public String getSourceDB() {
        return sourceDB;
    }

    public void setSourceDB(String sourceDB) {
        this.sourceDB = sourceDB;
    }

    public String getSourceTempTable() {
        return sourceTempTable;
    }

    public void setSourceTempTable(String sourceTempTable) {
        this.sourceTempTable = sourceTempTable;
    }

    public String getTargetDB() {
        return targetDB;
    }

    public void setTargetDB(String targetDB) {
        this.targetDB = targetDB;
    }

    public String getTargetTempTable() {
        return targetTempTable;
    }

    public void setTargetTempTable(String targetTempTable) {
        this.targetTempTable = targetTempTable;
    }

    public int getMaxRecords() {
        return maxRecords;
    }

    public void setMaxRecords(int maxRecords) {
        this.maxRecords = maxRecords;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean isActive) {
        this.isActive = isActive;
    }

    public List<Table> getTableList() {
        return tableList;
    }

    public void setTableList(List<Table> tableList) {
        this.tableList = tableList;
    }

    public List<Sequence> getSequenceList() {
        return sequenceList;
    }

    public void setSequenceList(List<Sequence> sequenceList) {
        this.sequenceList = sequenceList;
    }
}
