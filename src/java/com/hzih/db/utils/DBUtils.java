package com.hzih.db.utils;

import com.hzih.db.entity.FieldValue;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;

/**
 * Created by Administrator on 15-11-10.
 */
public class DBUtils {
    private final static Logger logger = Logger.getLogger(DBUtils.class);


    public final static int[] jdbcTypeArray = {
            Types.ARRAY, Types.BIGINT, Types.BINARY, Types.BIT, Types.BLOB,
            Types.CHAR, Types.CLOB, Types.DATE, Types.DECIMAL, Types.DISTINCT,
            Types.DOUBLE, Types.FLOAT, Types.INTEGER, Types.JAVA_OBJECT, Types.LONGVARBINARY,
            Types.LONGVARCHAR, Types.NULL, Types.NUMERIC, Types.OTHER, Types.REAL,
            Types.REF, Types.SMALLINT, Types.STRUCT, Types.TIME, Types.TIMESTAMP,
            Types.TINYINT, Types.VARBINARY, Types.VARCHAR
    };

    public final static String[] jdbcTypeStringArray = {
            "ARRAY", "BIGINT", "BINARY", "BIT", "BLOB",
            "CHAR", "CLOB", "DATE", "DECIMAL", "DISTINCT",
            "DOUBLE", "FLOAT", "INTEGER", "JAVA_OBJECT", "LONGVARBINARY",
            "LONGVARCHAR", "NULL", "NUMERIC", "OTHER", "REAL",
            "REF", "SMALLINT", "STRUCT", "TIME", "TIMESTAMP",
            "TINYINT", "VARBINARY", "VARCHAR"
    };

    public static int getJdbcType(String type) {
        int size = jdbcTypeStringArray.length;
        for (int i = 0; i < size; i++) {
            if (type.equalsIgnoreCase(jdbcTypeStringArray[i])) {
                return jdbcTypeArray[i];
            }
        }
        logger.error("unsupported jdbc type string:" + type);
        return Types.VARCHAR;
    }

    public static String getJdbcTypeString(int type) {
        int size = jdbcTypeArray.length;
        for (int i = 0; i < size; i++) {
            if (type == jdbcTypeArray[i]) {
                return jdbcTypeStringArray[i];
            }
        }

        logger.error("unsupported jdbc type indeEx:" + type);
        return "VARCHAR";
    }

    private static IDBUtils getIDBUtils(String appName, String dbType) {
        IDBUtils idbUtils = null;
        if("ORACLE".equalsIgnoreCase(dbType)){
            idbUtils = new OracleUtils();
        } else if("mssql".equalsIgnoreCase(dbType)){
            idbUtils = new MssqlUtils();
//        } else if("mysql".equalsIgnoreCase(dbType)){
//        } else if("db2".equalsIgnoreCase(dbType)){
//        } else if("sybase".equalsIgnoreCase(dbType)){
//        } else if("Invalid".equalsIgnoreCase(dbType)){

        } else {
            logger.warn(appName + "应用对应的数据库类型"+dbType+"未实现处理");
        }
        return idbUtils;
    }


    /**
     * 查询临时表语句
     * @param appName
     * @param dbType
     * @param tempTable
     * @param sourceTableName
     * @param maxRecords
     * @return
     */
    public static String createSql_SelectFromTemp(String appName,String dbType,String tempTable,String sourceTableName,int maxRecords){
        IDBUtils idbUtils = getIDBUtils(appName,dbType);
        return idbUtils.createSql_SelectFromTemp(tempTable,sourceTableName,maxRecords);
    }

    /**
     * 双向同步,sql语句条件中的主键值处理
     * @param appName
     * @param dbType
     * @param fieldType
     * @param pk
     * @return
     */
    public static String createSql_Where_pk_Twoway(String appName,
                                                   String dbType, String fieldType, String pk) {
        IDBUtils idbUtils = getIDBUtils(appName,dbType);
        return idbUtils.createSql_Where_pk_Twoway(appName,dbType,fieldType,pk);
    }

    public static PreparedStatement setPrepareStatement (String appName,String dbType,int idx,FieldValue fieldValue,
                                                  Connection connTarget, PreparedStatement prepStmt) throws SQLException{
        IDBUtils idbUtils = getIDBUtils(appName, dbType);
        return idbUtils.setPrepareStatement(idx, fieldValue, connTarget, prepStmt);
    }

    public static PreparedStatement setPrepareStatementPk(String appName,String dbType, int idx, String fieldType,
                                                          String pkName, String pkStr, PreparedStatement prepStmt) throws SQLException{
        IDBUtils idbUtils = getIDBUtils(appName, dbType);
        return idbUtils.setPrepareStatementPk(idx, fieldType, pkName, pkStr, prepStmt);
    }

    public static FieldValue setFieldValue(String appName,String dbType,String sourceTableName,
                                    FieldValue fieldValue, ResultSet rs) throws SQLException, IOException{
        IDBUtils idbUtils = getIDBUtils(appName, dbType);
        return idbUtils.setFieldValue(appName, sourceTableName, fieldValue, rs);
    }

    public static FieldValue setFieldValueTwoWay(String appName, String dbType, String sourceTableName,
                                                 FieldValue fieldValue, ResultSet rs, boolean isSourceToTarget) throws IOException, SQLException {
        IDBUtils idbUtils = getIDBUtils(appName, dbType);
        return idbUtils.setFieldValueTwoWay(appName, sourceTableName, fieldValue, rs,isSourceToTarget);
    }

    /**
     * 标记同步查找源表记录SQL语句
     * @param appName
     * @param dbType
     * @param sql_view_source
     * @param sourceTableName
     * @param flag
     * @param syncFlagValue
     * @return
     */
    public static String createSql_SelectFromSource(String appName, String dbType, String sql_view_source,
                                                    String sourceTableName, String flag, int syncFlagValue) {
        IDBUtils idbUtils = getIDBUtils(appName, dbType);
        return idbUtils.createSql_SelectFromSource(appName,sql_view_source,sourceTableName,flag, syncFlagValue);
    }

    /**
     *  时间标记同步查询最大或最小时间字段SQL语句
     * @param appName
     * @param dbType
     * @param sourceTableName
     * @param flag
     * @param isDesc
     * @return
     */
    public static String createSql_SelectDate(String appName, String dbType, String sourceTableName, String flag, boolean isDesc) {
        IDBUtils idbUtils = getIDBUtils(appName, dbType);
        return idbUtils.createSql_SelectDate(appName,dbType,sourceTableName,flag,isDesc);
    }

    /**
     *  时间标记同步查询源表SQL语句
     * @param appName
     * @param dbType
     * @param Sql_View_Source
     * @param sourceTableName
     * @param flag
     * @return
     */
    public static String createSql_SelectFromSourceDate(String appName, String dbType, String Sql_View_Source, String sourceTableName, String flag) {
        IDBUtils idbUtils = getIDBUtils(appName, dbType);
        return idbUtils.createSql_SelectFromSourceDate(appName,dbType,Sql_View_Source,sourceTableName,flag);
    }

}
