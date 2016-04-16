package com.hzih.db.entity;

/**
 * Created by Administrator on 15-11-9.
 */
public class Sequence {

    private String sourceSequenceName;
    private String targetSequenceName;

    public String getSourceSequenceName() {
        return sourceSequenceName;
    }

    public void setSourceSequenceName(String sourceSequenceName) {
        this.sourceSequenceName = sourceSequenceName;
    }

    public String getTargetSequenceName() {
        return targetSequenceName;
    }

    public void setTargetSequenceName(String targetSequenceName) {
        this.targetSequenceName = targetSequenceName;
    }
}
