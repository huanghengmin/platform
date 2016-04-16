package com.hzih.db.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 15-11-9.
 */
public class Table {

    private String sourceTableName;
    private String targetTableName;
    private String flagName;
    private int flagBefore;
    private int flagAfter;
    private boolean isSourceDeleteAble;
    private boolean isOnlyOnce;
    private boolean isTargetDeleteAble;
    private boolean isTargetOnlyInsert;
    private int tableSeqNumber;

    private List<FieldValue> fieldValueList = new ArrayList<FieldValue>();

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public String getFlagName() {
        return flagName;
    }

    public void setFlagName(String flagName) {
        this.flagName = flagName;
    }

    public int getFlagBefore() {
        return flagBefore;
    }

    public void setFlagBefore(int flagBefore) {
        this.flagBefore = flagBefore;
    }

    public int getFlagAfter() {
        return flagAfter;
    }

    public void setFlagAfter(int flagAfter) {
        this.flagAfter = flagAfter;
    }

    public boolean isSourceDeleteAble() {
        return isSourceDeleteAble;
    }

    public void setSourceDeleteAble(boolean isSourceDeleteAble) {
        this.isSourceDeleteAble = isSourceDeleteAble;
    }

    public boolean isOnlyOnce() {
        return isOnlyOnce;
    }

    public void setOnlyOnce(boolean isOnlyOnce) {
        this.isOnlyOnce = isOnlyOnce;
    }

    public boolean isTargetDeleteAble() {
        return isTargetDeleteAble;
    }

    public void setTargetDeleteAble(boolean isTargetDeleteAble) {
        this.isTargetDeleteAble = isTargetDeleteAble;
    }

    public boolean isTargetOnlyInsert() {
        return isTargetOnlyInsert;
    }

    public void setTargetOnlyInsert(boolean isTargetOnlyInsert) {
        this.isTargetOnlyInsert = isTargetOnlyInsert;
    }

    public int getTableSeqNumber() {
        return tableSeqNumber;
    }

    public void setTableSeqNumber(int tableSeqNumber) {
        this.tableSeqNumber = tableSeqNumber;
    }

    public List<FieldValue> getFieldValueList() {
        return fieldValueList;
    }

    public void setFieldValueList(List<FieldValue> fieldValueList) {
        this.fieldValueList = fieldValueList;
    }
}
