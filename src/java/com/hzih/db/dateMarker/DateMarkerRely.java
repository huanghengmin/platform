package com.hzih.db.dateMarker;

import com.hzih.db.entity.*;
import com.hzih.db.utils.DBUtils;
import com.hzih.jdbc.DataSourceUtil;
import com.inetec.common.config.nodes.Jdbc;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.io.*;
import java.sql.*;
import java.util.*;


/**
 * Created by Administrator on 15-10-29.
 */
public class DateMarkerRely extends Thread {

    private static final Logger logger = Logger.getLogger(DateMarkerRely.class);

    private boolean isRun = false;
    private boolean isStop = false;
    private Type type;
    private DataSource dataSource = null;
    private DataSource dataTarget = null;
    private String appName;
    private int maxRecords;

    private Map<String, String> Sql_SelectCountSourceMap;
    private Map<String, String> Sql_SelectFromSourceMap;
    private Map<String, String> Sql_SelectFromTargetMap;
    private Map<String, String> Sql_InsertToTargetMap;
    private Map<String, String> Sql_UpdateToTargetMap;
    private Map<String, String> Sql_SelectEndDateMap;
    private Map<String, String> Sql_SelectStartDateMap;
    private Map<String, String> Sql_UpdateToSourceMap;
    private Map<String, List<FieldValue>> sourceTablePkFieldListMap;
    private Map<String, List<FieldValue>> fieldsMap;
    private Map<String, Table> tableMap;
    private Map<String, String> targetTableNameMap;

    private String sourceDbType;
    private String targetDbType;

    public void config(Jdbc sourceJdbc, Jdbc targetJdbc) {
        this.sourceDbType = sourceJdbc.getDbType();
        this.targetDbType = targetJdbc.getDbType();
    }

    public void init(Type type) {
        this.type = type;
        this.appName = type.getAppName();
        try {
            dataSource = DataSourceUtil.getDataSource(DataSourceUtil.DRUID_SOURCE);
            dataTarget = DataSourceUtil.getDataSource(DataSourceUtil.DRUID_TARGET);
        } catch (Exception e) {
            logger.error(appName+"建立数据源出错"+e.getMessage(),e);
        }

        maxRecords = type.getMaxRecords();
        Sql_SelectCountSourceMap = new HashMap<String, String>();
        Sql_SelectFromSourceMap = new HashMap<String, String>();
        Sql_SelectFromTargetMap = new HashMap<String, String>();
        Sql_SelectEndDateMap = new HashMap<String, String>();
        Sql_SelectStartDateMap = new HashMap<String, String>();
        Sql_InsertToTargetMap = new HashMap<String, String>();
        Sql_UpdateToTargetMap = new HashMap<String, String>();
        Sql_UpdateToSourceMap = new HashMap<String, String>();
        sourceTablePkFieldListMap = new HashMap<String, List<FieldValue>>();
        fieldsMap = new HashMap<String, List<FieldValue>>();
        tableMap = new HashMap<String, Table>();
        targetTableNameMap = new HashMap<String, String>();

        List<Table> tableList = type.getTableList();
        for (Table table : tableList) {
            String sourceTableName = table.getSourceTableName();
            String targetTableName = table.getTargetTableName();

            List<FieldValue> fields = table.getFieldValueList();
            String flag = "";
            String Sql_View_Source = "";
            String Sql_View_Lob = "";
            String Sql_View_Target = "";
            String Sql_InsertToTarget_Values = "";
            String Sql_UpdateToTarget_Values = " ";
            String Sql_TargetSelect_Where = " ";
            int idx = 0;
            List<FieldValue> sourceTablePkFieldList = new ArrayList<FieldValue>();
            for (FieldValue field : fields) {
                if (field.isPk()) {
                    sourceTablePkFieldList.add(field);
                    flag = field.getFieldName();
                }
                idx++;
                if (idx < fields.size()) {
                    Sql_View_Source += field.getFieldName() + ","; //源端字段列表
                    Sql_View_Target += field.getDestField() + ","; //目标端字段列表
                    Sql_InsertToTarget_Values += "?,"; //目标端插入动态值
                    Sql_UpdateToTarget_Values += field.getDestField() + " =? , "; //目标端更新动态值
                } else {
                    Sql_View_Source += field.getFieldName();
                    Sql_View_Target += field.getDestField();
                    Sql_InsertToTarget_Values += "?";
                    Sql_UpdateToTarget_Values += field.getDestField() + " =? ";
                }
            }

            for (int y = 0; y < sourceTablePkFieldList.size(); y++) {
                FieldValue field = sourceTablePkFieldList.get(y);
                Sql_TargetSelect_Where += field.getDestField() + "=?";
                if (y != sourceTablePkFieldList.size() - 1) {
                    Sql_TargetSelect_Where += ",";
                }
            }

            String Sql_SelectEndDate = DBUtils.createSql_SelectDate(appName,sourceDbType,sourceTableName,flag,true);
//            String Sql_SelectEndDate = "SELECT " + flag + " FROM (SELECT " + flag + ",ROWNUM rn FROM " + sourceTableName + " ORDER BY " + flag + " DESC) WHERE rn = 1";

            String Sql_SelectStartDate = DBUtils.createSql_SelectDate(appName,sourceDbType,sourceTableName,flag,false);
//            String Sql_SelectStartDate = "SELECT " + flag + " FROM (SELECT " + flag + ",ROWNUM rn FROM " + sourceTableName + " ORDER BY " + flag + " ASC) WHERE rn = 1";

            String Sql_SelectCountSource = "SELECT COUNT(*) FROM " + sourceTableName + " WHERE " + flag + " between to_timestamp(?, 'yyyy-mm-dd hh24:mi:ss.ff') and to_timestamp(?, 'yyyy-mm-dd hh24:mi:ss.ff')"; //查询源端总记录条数

            String Sql_SelectFromSource = DBUtils.createSql_SelectFromSourceDate(appName,sourceDbType,Sql_View_Source,sourceTableName,flag);
//            String Sql_SelectFromSource = "SELECT * FROM ( SELECT " + Sql_View_Source + ", ROWNUM RN " + "FROM (" + "SELECT " + Sql_View_Source + " FROM " + sourceTableName + " WHERE " + flag + " between to_timestamp(?, 'yyyy-mm-dd hh24:mi:ss.ff') and to_timestamp(?, 'yyyy-mm-dd hh24:mi:ss.ff')" + ")  WHERE ROWNUM <= ? ) WHERE RN >= ?";  //源表查询

            String Sql_SelectFromTarget = "SELECT " + Sql_View_Target + " FROM " + targetTableName + " WHERE " + Sql_TargetSelect_Where; //目标端查询语句需要加入where条件

            String Sql_InsertToTarget = "INSERT INTO " + targetTableName + " (" + Sql_View_Target + ") VALUES (" + Sql_InsertToTarget_Values + ")";  //目标插入动态语句

            String Sql_UpdateToTarget = "UPDATE " + targetTableName + " SET " + Sql_UpdateToTarget_Values + " WHERE " + Sql_TargetSelect_Where;   //目标更新动态语句

            Sql_SelectCountSourceMap.put(sourceTableName, Sql_SelectCountSource);
            Sql_SelectFromSourceMap.put(sourceTableName, Sql_SelectFromSource);
            Sql_SelectFromTargetMap.put(sourceTableName, Sql_SelectFromTarget);
            Sql_InsertToTargetMap.put(sourceTableName, Sql_InsertToTarget);
            Sql_UpdateToTargetMap.put(sourceTableName, Sql_UpdateToTarget);
            Sql_SelectEndDateMap.put(sourceTableName, Sql_SelectEndDate);
            Sql_SelectStartDateMap.put(sourceTableName, Sql_SelectStartDate);
            sourceTablePkFieldListMap.put(sourceTableName, sourceTablePkFieldList);
            fieldsMap.put(sourceTableName, fields);
            tableMap.put(sourceTableName, table);
            targetTableNameMap.put(sourceTableName, targetTableName);
        }
    }

    public boolean isRun() {
        return isRun;
    }

    public void stopThread() {
        isStop = true;
    }

    /**
     * //取真实数据
     * //转换成IUD 三种sql语句
     * //提交给目标端
     */
    @Override
    public void run() {
        isRun = true;
        while (isRun) {
            if (!isStop) {
                do {
                    int interval = 60 * 1000;
                    for (Table table : type.getTableList()) {
                        String sourceTableName = table.getSourceTableName();
                        String startDate = null;
                        Long startLongTime = null;
                        String path = System.getProperty("ichange.home") + "/data/" + appName + "/" + sourceTableName + ".properties";
                        File file = new File(path);
                        Properties pros = new Properties();
                        if (file.exists()) {
                            try {
                                FileInputStream ins = new FileInputStream(file);
                                if (ins != null) {
                                    try {
                                        pros.load(ins);
                                        startDate = pros.getProperty("time");
                                    } catch (IOException e) {
                                        logger.error(appName+"获取数据同步开始时间出错"+e.getMessage(), e);
                                    }
                                    try {
                                        ins.close();
                                    } catch (IOException e) {
                                        logger.error(appName+"关闭属性文件出错"+e.getMessage(), e);
                                    }
                                }
                            } catch (FileNotFoundException e) {
                                logger.error(appName+"属性文件未找到异常"+e.getMessage(), e);
                            }
                        }
                        if (startDate == null) {
                            Timestamp startTimestamp = selectStartDate(sourceTableName);
                            startLongTime = startTimestamp.getTime();
                        } else {
                            Timestamp startTimestamp = Timestamp.valueOf(startDate);
                            startLongTime = startTimestamp.getTime();
                        }
                        if (startLongTime != null) {
                            Timestamp endTimeStamp = selectEndDate(sourceTableName);
                            for (long y = startLongTime; (y + interval) < endTimeStamp.getTime(); y += interval) {
                                Long endLongTime = y + interval;
                                Timestamp endTime = new Timestamp(endLongTime);
                                String endDate = endTime.toString();
                                int count = selectCountSource(startDate, endDate, sourceTableName); //总条数
                                int pages = count / maxRecords;
                                pages += (int)Math.ceil(count % maxRecords);
                                for (int i = 0; i < pages; i += 1) {
                                    int start = i * maxRecords + 1;
                                    int end = (i + 1) * maxRecords;
                                    if (end > count) {
                                        end = count;
                                    }


                                    processBatch(startDate, endDate, start, end, sourceTableName);

                                    File f = new File(path);
                                    if (f.exists()) {
                                        try {
                                            FileInputStream ins = new FileInputStream(f);
                                            try {
                                                pros.load(ins);
                                                if (ins != null) {
                                                    ins.close();
                                                }
                                                pros.setProperty("time", endDate);
                                                OutputStream fos = new FileOutputStream(f);
                                                pros.store(fos, "Update time value" + endDate);
                                                if (fos != null) {
                                                    fos.close();
                                                }
                                            } catch (IOException e) {
                                                logger.error(appName+"加载属性文件出错"+e.getMessage(), e);
                                            }
                                        } catch (FileNotFoundException e) {
                                            logger.error(appName+"属性文件示查找到异常"+e.getMessage(), e);
                                        }
                                    } else {
                                        f.mkdirs();
                                        OutputStream fos = null;
                                        try {
                                            fos = new FileOutputStream(f);
                                            Properties properties = new Properties();
                                            try {
                                                properties.store(fos, "Update time value" + endDate);
                                            } catch (IOException e) {
                                                logger.error(appName+"更新同步开始时间出错"+e.getMessage(), e);
                                            }
                                            if (fos != null) {
                                                try {
                                                    fos.close();
                                                } catch (IOException e) {
                                                    logger.error(appName+"关闭属性文件流出错"+e.getMessage(),e);
                                                }
                                            }
                                        } catch (FileNotFoundException e) {
                                            logger.error(appName+"属性文件未找到异常"+e.getMessage(), e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    try {
                        Thread.sleep(1000 * type.getInterval());
                    } catch (InterruptedException e) {
                        logger.error(appName+"线程同休眠间隔出错"+e.getMessage(), e);
                    }
                } while (!isStop);
            } else {
                isRun = false;
            }
        }
    }


    /**
     * @param start
     * @param end
     * @param sourceTableName
     */
    private void processBatch(String startDate, String endDate, int start, int end, String sourceTableName) {

        List<List<FieldValue>> rows = selectFromSource(startDate, endDate, start, end, sourceTableName);
        if (rows.size() > 0) {
            Table table = tableMap.get(sourceTableName);
            if (table.isTargetOnlyInsert()) {
                processInsertRows(rows, sourceTableName);
            } else {
                CompareRows compareRows = listCompareTarget(rows, sourceTableName);
                if (compareRows != null) {
                    List<List<FieldValue>> insertRows = compareRows.getInsertRows();
                    List<List<FieldValue>> updateRows = compareRows.getUpdateRows();
                    if (insertRows.size() > 0) {
                        processInsertRows(insertRows, sourceTableName);
                    }
                    if (updateRows.size() > 0) {
                        processUpdateRows(updateRows, sourceTableName);
                    }
                }
            }
        }
    }

    /**
     * 查询源端总记录条数
     *
     * @return
     */
    private int selectCountSource(String startDate, String endDate, String sourceTableName) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            String Sql_SelectCountSource = Sql_SelectCountSourceMap.get(sourceTableName);
            conn = dataSource.getConnection();
            preparedStatement = conn.prepareStatement(Sql_SelectCountSource);
            preparedStatement.setString(1, startDate);
            preparedStatement.setString(2, endDate);
            rs = preparedStatement.executeQuery();
            int rowCount = 0;
            if (rs.next()) {
                rowCount = rs.getInt(1);
                return rowCount;
            }
        } catch (Exception e) {
            logger.error("应用" + appName + "读取表" + sourceTableName + "的记录总数失败,原因:" + e.getMessage(), e);
            return 0;
        } finally {
            closeJdbc(conn, preparedStatement, rs);
        }
        return 0;
    }


    private Timestamp selectEndDate(String sourceTableName) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            String Sql_SelectEndDate = Sql_SelectEndDateMap.get(sourceTableName);
            conn = dataSource.getConnection();
            preparedStatement = conn.prepareStatement(Sql_SelectEndDate);
            rs = preparedStatement.executeQuery();
            Timestamp timestamp;
            if (rs.next()) {
                timestamp = rs.getTimestamp(1);
                return timestamp;
            }
        } catch (Exception e) {
            logger.error("应用" + appName + "读取表" + sourceTableName + "结果日期,原因:" + e.getMessage(), e);
            return null;
        } finally {
            closeJdbc(conn, preparedStatement, rs);
        }
        return null;
    }


    private Timestamp selectStartDate(String sourceTableName) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            String Sql_SelectStartDate = Sql_SelectStartDateMap.get(sourceTableName);
            conn = dataSource.getConnection();
            preparedStatement = conn.prepareStatement(Sql_SelectStartDate);
            rs = preparedStatement.executeQuery();
            Timestamp timestamp;
            if (rs.next()) {
                timestamp = rs.getTimestamp(1);
                return timestamp;
            }
        } catch (Exception e) {
            logger.error("应用" + appName + "读取表" + sourceTableName + "的记录总数失败,原因:" + e.getMessage(), e);
            return null;
        } finally {
            closeJdbc(conn, preparedStatement, rs);
        }
        return null;
    }

    /**
     * 处理插入数据集合
     */
    private void processInsertRows(List<List<FieldValue>> insertRows, String sourceTableName) {
        boolean isSuccessToTarget = false;
        Connection connTarget = null;
        PreparedStatement prepStmt = null;
        try {
            String Sql_InsertToTarget = Sql_InsertToTargetMap.get(sourceTableName);
            connTarget = dataTarget.getConnection();
            connTarget.setAutoCommit(false);
            prepStmt = connTarget.prepareStatement(Sql_InsertToTarget);
            if (insertRows != null) {
                for (List<FieldValue> fieldValues : insertRows) {
                    int idx = 0;
                    for (FieldValue fieldValue : fieldValues) {
                        idx ++;
                        prepStmt = DBUtils.setPrepareStatement(appName,targetDbType,idx,fieldValue,connTarget,prepStmt);
                    }
                    prepStmt.addBatch();
                }
                prepStmt.executeBatch();
                prepStmt.clearBatch();
                connTarget.commit();
                isSuccessToTarget = true;
            }

        } catch (Exception e) {
            logger.error(appName+"插入目标端结果集出错"+e.getMessage(), e);
            try {
                connTarget.rollback();
            } catch (SQLException e1) {
                logger.error(appName+"目标端数据回滚出错"+e.getMessage(),e);
            }
            isSuccessToTarget = false;
        } finally {
            if (connTarget != null) {
                try {
                    connTarget.setAutoCommit(true);
                } catch (SQLException e) {
                    logger.error(appName+"设置数据库自动提交出错"+e.getMessage(),e);
                }
            }
            closeJdbc(connTarget, prepStmt, null);
        }
        if (isSuccessToTarget) {
            String targetTableName = targetTableNameMap.get(sourceTableName);
            logger.info(appName + " 同步" + sourceTableName + "到" + targetTableName + "了" + insertRows.size() + "条" + System.currentTimeMillis());
        }
    }

    /**
     * 处理更新数据集合
     *
     * @param updateRows
     * @param sourceTableName
     */
    private void processUpdateRows(List<List<FieldValue>> updateRows, String sourceTableName) {
        boolean isSuccessToTarget = false;
        Connection connTarget = null;
        PreparedStatement prepStmt = null;
        try {
            String Sql_UpdateToTarget = Sql_UpdateToTargetMap.get(sourceTableName);
            connTarget = dataTarget.getConnection();
            connTarget.setAutoCommit(false);
            prepStmt = connTarget.prepareStatement(Sql_UpdateToTarget);
            if (updateRows != null) {
                for (List<FieldValue> fieldValues : updateRows) {
                    int idx = 0;
                    List<FieldValue> pkFieldValues = new ArrayList<>();
                    for (FieldValue fieldValue : fieldValues) {
                        if (fieldValue.isPk()) {
                            pkFieldValues.add(fieldValue);
                        }
                        idx ++;
                        prepStmt = DBUtils.setPrepareStatement(appName,targetDbType,idx,fieldValue,connTarget,prepStmt);
                    }
                    for (int i = 0; i < pkFieldValues.size(); i++) {
                        FieldValue fieldValue = pkFieldValues.get(i);
                        idx ++;
                        prepStmt = DBUtils.setPrepareStatement(appName,targetDbType,idx,fieldValue,connTarget,prepStmt);
                    }
                    prepStmt.addBatch();
                }
                prepStmt.executeBatch();
                prepStmt.clearBatch();
                connTarget.commit();
                isSuccessToTarget = true;
            }
        } catch (Exception e) {
            logger.error(appName+"更新目标端数据库出错"+e.getMessage(), e);
            try {
                connTarget.rollback();
            } catch (SQLException e1) {
                logger.error(appName+"目标端数据回滚出错"+e1.getMessage(),e1);
            }
            isSuccessToTarget = false;
        } finally {
            if (connTarget != null) {
                try {
                    connTarget.setAutoCommit(true);
                } catch (SQLException e) {
                    logger.error(appName+"设置数据库自动提交出错"+e.getMessage(),e);
                }
            }
            closeJdbc(connTarget, prepStmt, null);
        }
        if (isSuccessToTarget) {
            String targetTableName = targetTableNameMap.get(sourceTableName);
            logger.info(appName + " 同步更新" + sourceTableName + "到" + targetTableName + "了" + updateRows.size() + "条" + System.currentTimeMillis());
        }
    }

    /**
     * 获取源表记录
     *
     * @return
     */
    private List<List<FieldValue>> selectFromSource(String startDate, String endDate, int start, int end, String sourceTableName) {
        List<List<FieldValue>> sourceValueLists = new ArrayList<List<FieldValue>>();
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            String Sql_SelectFromSource = Sql_SelectFromSourceMap.get(sourceTableName);
            List<FieldValue> fields = fieldsMap.get(sourceTableName);
            conn = dataSource.getConnection();
            preparedStatement = conn.prepareStatement(Sql_SelectFromSource);
            preparedStatement.setString(1, startDate);
            preparedStatement.setString(2, endDate);
            preparedStatement.setInt(3, end);
            preparedStatement.setInt(4, start);
            rs = preparedStatement.executeQuery();
            while (rs.next()) {
                List<FieldValue> sourceValueList = new ArrayList<FieldValue>();
                for (FieldValue fieldValue : fields) {
                    FieldValue value = new FieldValue();
                    value.setColumnSize(String.valueOf(fieldValue.getColumnSize()));
                    value.setDbType(fieldValue.getDbType());
                    value.setDestField(fieldValue.getDestField());
                    value.setFieldName(fieldValue.getFieldName());
                    value.setJdbcType(fieldValue.getJdbcType());
                    value.setNull(String.valueOf(fieldValue.isNull()));
                    value.setPk(String.valueOf(fieldValue.isPk()));
                    FieldValue f = DBUtils.setFieldValue(appName,sourceDbType,sourceTableName,value,rs);
                    sourceValueList.add(f);
                }
                sourceValueLists.add(sourceValueList);
            }
        } catch (Exception e) {
            logger.error("应用" + appName + "读取表" + sourceTableName + "的记录失败,原因:" + e.getMessage(), e);
        } finally {
            closeJdbc(conn, preparedStatement, rs);
        }
        return sourceValueLists;
    }



    /**
     * 对比目标集合
     *
     * @param rows
     * @param sourceTableName
     * @return
     */
    private CompareRows listCompareTarget(List<List<FieldValue>> rows, String sourceTableName) {
        List<List<FieldValue>> insertRows = new ArrayList<>();
        List<List<FieldValue>> updateRows = new ArrayList<>();
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        try {
            String Sql_SelectFromTarget = Sql_SelectFromTargetMap.get(sourceTableName);
            conn = dataTarget.getConnection();
            preparedStatement = conn.prepareStatement(Sql_SelectFromTarget);
            for (List<FieldValue> row : rows){
                ResultSet rs = null;
                try {
                    int idx = 0;
                    for (FieldValue fieldValue : row) {
                        if (fieldValue.isPk()) {
                            idx ++;
                            preparedStatement = DBUtils.setPrepareStatement(appName,targetDbType,idx, fieldValue,conn.getMetaData().getConnection(), preparedStatement);
                        }
                    }
                    rs = preparedStatement.executeQuery();
                    if (rs!=null&&rs.next()) {
                        updateRows.add(row);
                    }else {
                        insertRows.add(row);
                    }
                    rs.close();
                } catch (SQLException e) {
                    logger.error(appName+"对比目标端记录出错"+e.getMessage(),e);
                }
            }
            closeJdbc(conn, preparedStatement, null);
            CompareRows compareRows = new CompareRows(insertRows, updateRows);
            return compareRows;
        } catch (SQLException e) {
            logger.error(appName+"遍历对比目标端结果集出错"+e.getMessage(),e);
            return null;
        }
    }

    /**
     * @param conn
     * @param statement
     * @param rs
     */
    private void closeJdbc(Connection conn, Statement statement, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                logger.error(appName+"关闭数据库结果集出错"+e.getMessage(), e);
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                logger.error(appName+"关闭数据库会话出错"+e.getMessage(), e);
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error(appName+"关闭数据库连接出错"+e.getMessage(),e);
            }
        }
    }
}
