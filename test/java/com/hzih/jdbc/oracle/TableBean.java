package com.hzih.jdbc.oracle;

/**
 * Created by Administrator on 15-11-6.
 */
public class TableBean {
    private String tableName;
    private boolean isView;// 是否是视图

    public TableBean() {
    }

    public TableBean(String tableName, boolean isView) {
        this.tableName = tableName;
        this.isView = isView;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isView() {
        return isView;
    }

    public void setView(boolean isView) {
        this.isView = isView;
    }
}
