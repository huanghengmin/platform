/*=============================================================
 * 文件名称: TempRow.java
 * 版    本: 1.0
 * 作    者: bluewind
 * 创建时间: 2005-11-12
 * ============================================================
 * <p>版权所有  (c) 2005 杭州网科信息工程有限公司</p>
 * <p>
 * 本源码文件作为杭州网科信息工程有限公司所开发软件一部分，它包涵
 * 了本公司的机密很所有权信息，它只提供给本公司软件的许可用户使用。
 * </p>
 * <p>
 * 对于本软件使用，必须遵守本软件许可说明和限制条款所规定的期限和
 * 条件。
 * </p>
 * <p>
 * 特别需要指出的是，您可以从本公司软件，或者该软件的部件，或者合
 * 作商取得并有权使用本程序。但是不得进行复制或者散发本文件，也不
 * 得未经本公司许可修改使用本文件，或者进行基于本程序的开发，否则
 * 我们将在最大的法律限度内对您侵犯本公司版权的行为进行起诉。
 * </p>
 * ==========================================================*/
package com.hzih.db.entity;

import com.inetec.common.exception.Ex;

import java.sql.Timestamp;


public class TempRow {
    private long id;
    private String databaseName;
    private String tableName;

    private String pks;
    private String act;
    private Timestamp m_actTime;

    public TempRow() {
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPks() {
        return pks;
    }

    public void setPks(String pkString) throws Ex {
        pks = pkString;
    }

    public String getAct() {
        return act;
    }

    public void setAct(String act) {
        this.act = act;
    }


    public void setActTime(Timestamp actTime) {
        m_actTime = actTime;
    }

    public Timestamp getActTime() {
        return m_actTime;
    }
}
