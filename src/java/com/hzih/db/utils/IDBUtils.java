package com.hzih.db.utils;

import com.hzih.db.entity.FieldValue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Administrator on 15-11-10.
 */
public interface IDBUtils {

    public String createSql_SelectFromTemp(String tempTable,String sourceTableName,int maxRecords);

    public String createSql_Where_pk_Twoway(String appName, String dbType, String fieldType, String pk);

    public PreparedStatement setPrepareStatement (int idx,FieldValue fieldValue,
                                                         Connection connTarget, PreparedStatement prepStmt) throws SQLException;

    public PreparedStatement setPrepareStatementPk(int idx, String fieldType,
                                                   String pkName, String pkStr, PreparedStatement prepStmt) throws SQLException;

    public FieldValue setFieldValue(String appName,String sourceTableName,
                                           FieldValue fieldValue, ResultSet rs) throws SQLException, IOException;

    public FieldValue setFieldValueTwoWay(String appName, String sourceTableName,
                                          FieldValue fieldValue, ResultSet rs, boolean isSourceToTarget) throws SQLException, IOException;

    public String createSql_SelectFromSource(String appName, String sql_view_source, String sourceTableName, String flag, int syncFlagValue);

    public String createSql_SelectDate(String appName, String dbType, String sourceTableName, String flag, boolean isDesc);

    public String createSql_SelectFromSourceDate(String appName, String dbType, String sql_view_source, String sourceTableName, String flag);
}
