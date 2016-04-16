package com.hzih.db.entity;

/**
 * Created by Administrator on 15-11-3.
 */
public class SequenceValue {

    public static final String ALL_SEQUENCES = "ALL_SEQUENCES";
    public static final String SEQUENCE_NAME = "SEQUENCE_NAME";
    public static final String SEQUENCE_OWNER = "SEQUENCE_OWNER";
    public static final String LAST_NUMBER = "LAST_NUMBER";
    public static final String INCREMENT_BY = "INCREMENT_BY";
    public static final String CACHE_SIZE = "CACHE_SIZE";
    public static final String CURRVAL = "CURRVAL";

    private long lastNumber = 0;
    private long increment = 0;
    private long cacheSize = 0;

    public long getLastNumber() {
        return lastNumber;
    }

    public void setLastNumber(long lastNumber) {
        this.lastNumber = lastNumber;
    }

    public long getIncrement() {
        return increment;
    }

    public void setIncrement(long increment) {
        this.increment = increment;
    }

    public long getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
    }
}
