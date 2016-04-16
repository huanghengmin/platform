package com.hzih.db.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 15-11-2.
 */
public class CompareRows {
    List<List<FieldValue>> insertRows = new ArrayList<>();
    List<List<FieldValue>> updateRows = new ArrayList<>();

    public CompareRows(List<List<FieldValue>> insertRows, List<List<FieldValue>> updateRows) {
        this.insertRows = insertRows;
        this.updateRows = updateRows;
    }

    public List<List<FieldValue>> getInsertRows() {
        return insertRows;
    }

    public void setInsertRows(List<List<FieldValue>> insertRows) {
        this.insertRows = insertRows;
    }

    public List<List<FieldValue>> getUpdateRows() {
        return updateRows;
    }

    public void setUpdateRows(List<List<FieldValue>> updateRows) {
        this.updateRows = updateRows;
    }
}
