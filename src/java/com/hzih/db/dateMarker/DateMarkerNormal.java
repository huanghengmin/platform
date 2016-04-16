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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * Created by Administrator on 15-10-29.
 */
public class DateMarkerNormal extends Thread {

    private static final Logger logger = Logger.getLogger(DateMarkerNormal.class);

    private boolean isRun = false;
    private boolean isStop = false;
    private Type type;
    private DataSource dataSource = null;
    private DataSource dataTarget = null;
    private String appName;

    private String sourceTableName;
    private String targetTableName;
    private Table table;
    private int maxRecords;
    private List<FieldValue> sourceTablePkFieldList;
    private int pkSize;
    private String Sql_SelectFromSource;
    private String Sql_SelectCountSource;
    private String Sql_SelectFromTarget;
    private String Sql_InsertToTarget;
    private String Sql_UpdateToTarget;
    private String Sql_SelectEndDate;
    private String Sql_SelectStartDate;

    private List<FieldValue> fields;

    private String sourceDbType;
    private String targetDbType;

    public void config(Jdbc sourceJdbc, Jdbc targetJdbc) {
        this.sourceDbType = sourceJdbc.getDbType();
        this.targetDbType = targetJdbc.getDbType();
    }

    public void init(Type type, String sourceTableName, String targetTableName, Table table) {
        this.type = type;
        this.appName = type.getAppName();
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.table = table;
        try {
            dataSource = DataSourceUtil.getDataSource(DataSourceUtil.DRUID_SOURCE);
            dataTarget = DataSourceUtil.getDataSource(DataSourceUtil.DRUID_TARGET);
        } catch (Exception e) {
            logger.error(appName+"建立数据源出错"+e.getMessage(),e);
        }


        maxRecords = type.getMaxRecords();
        fields = table.getFieldValueList();
        String flag = "";
        String Sql_View_Source = "";
        String Sql_View_Target = "";
        String Sql_InsertToTarget_Values = "";
        String Sql_UpdateToTarget_Values = " ";
        String Sql_TargetSelect_Where = " ";
        int idx = 0;
        sourceTablePkFieldList = new ArrayList<FieldValue>();
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

        for (int i = 0; i < sourceTablePkFieldList.size(); i++) {
            FieldValue field = sourceTablePkFieldList.get(i);
            Sql_TargetSelect_Where += field.getDestField() + "=?";
            if (i != sourceTablePkFieldList.size() - 1) {
                Sql_TargetSelect_Where += ",";
            }
        }

        Sql_SelectEndDate = DBUtils.createSql_SelectDate(appName, sourceDbType, sourceTableName, flag, true);
//        Sql_SelectEndDate = "SELECT " + flag + " FROM (SELECT " + flag + ",ROWNUM rn FROM " + sourceTableName + " ORDER BY " + flag + " DESC) WHERE rn = 1";
        Sql_SelectStartDate = DBUtils.createSql_SelectDate(appName,sourceDbType,sourceTableName,flag,false);
//        Sql_SelectStartDate = "SELECT " + flag + " FROM (SELECT " + flag + ",ROWNUM rn FROM " + sourceTableName + " ORDER BY " + flag + " ASC) WHERE rn = 1";

        Sql_SelectCountSource = "SELECT COUNT(*) FROM " + sourceTableName + " WHERE " + flag
                + " between to_timestamp(?, 'yyyy-mm-dd hh24:mi:ss.ff') and to_timestamp(?, 'yyyy-mm-dd hh24:mi:ss.ff')"; //查询源端总记录条数

        Sql_SelectFromSource = DBUtils.createSql_SelectFromSourceDate(appName, sourceDbType, Sql_View_Source, sourceTableName, flag);
//        Sql_SelectFromSource = "SELECT * FROM ( SELECT " + Sql_View_Source + ", ROWNUM RN " + "FROM ("
//                + "SELECT " + Sql_View_Source + " FROM " + sourceTableName + " WHERE " + flag
//                + " between to_timestamp(?, 'yyyy-mm-dd hh24:mi:ss.ff') and to_timestamp(?, 'yyyy-mm-dd hh24:mi:ss.ff')"
//                + ")  WHERE ROWNUM <= ? ) WHERE RN >= ?";  //源表查询

        Sql_SelectFromTarget = "SELECT " + Sql_View_Target + " FROM " + targetTableName + " WHERE " + Sql_TargetSelect_Where; //目标端查询语句需要加入where条件

        Sql_InsertToTarget = "INSERT INTO " + targetTableName + " (" + Sql_View_Target + ") VALUES (" + Sql_InsertToTarget_Values + ")";  //目标插入动态语句

        Sql_UpdateToTarget = "UPDATE " + targetTableName + " SET " + Sql_UpdateToTarget_Values + " WHERE " + Sql_TargetSelect_Where;   //目标更新动态语句

        pkSize = sourceTablePkFieldList.size();//主键个数
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
                        Timestamp startTimestamp = selectStartDate();
                        startLongTime = startTimestamp.getTime();
                    } else {
                        Timestamp startTimestamp = Timestamp.valueOf(startDate);
                        startLongTime = startTimestamp.getTime();
                    }
                    if (startLongTime != null) {
                        Timestamp endTimeStamp = selectEndDate();
                        for (long y = startLongTime; (y + interval) < endTimeStamp.getTime(); y += interval) {
                            Long endLongTime = y + interval;
                            Timestamp endTime = new Timestamp(endLongTime);
                            String endDate = endTime.toString();
                            int count = selectCountSource(startDate, endDate); //总条数
                            int pages = count / maxRecords;
                            pages += (int)Math.ceil(count % maxRecords);
                            for (int i = 0; i < pages; i += 1) {
                                int start = i * maxRecords + 1;
                                int end = (i + 1) * maxRecords;
                                if (end > count) {
                                    end = count;
                                }
                                processBatch(startDate, endDate, start, end);
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
                        try {
                            Thread.sleep(1000 * type.getInterval());
                        } catch (InterruptedException ex) {
                            logger.error(appName+"线程同休眠间隔出错"+ex.getMessage(), ex);
                        }
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
     */
    private void processBatch(String startDate, String endDate, int start, int end) {
        List<List<FieldValue>> rows = selectFromSource(startDate, endDate, start, end);
        if (rows.size() > 0) {
            if (table.isTargetOnlyInsert()) {
                processInsertRows(rows);
            } else {
                CompareRows compareRows = listCompareTarget(rows);
                if (compareRows != null) {
                    List<List<FieldValue>> insertRows = compareRows.getInsertRows();
                    List<List<FieldValue>> updateRows = compareRows.getUpdateRows();
                    if (insertRows.size() > 0) {
                        processInsertRows(insertRows);
                    }
                    if (updateRows.size() > 0) {
                        processUpdateRows(updateRows);
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
    private int selectCountSource(String startDate, String endDate) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
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


    private Timestamp selectEndDate() {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
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

    private Timestamp selectStartDate() {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
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
    private void processInsertRows(List<List<FieldValue>> insertRows) {
        boolean isSuccessToTarget = false;
        Connection connTarget = null;
        PreparedStatement prepStmt = null;
        try {
            connTarget = dataTarget.getConnection();
            connTarget.setAutoCommit(false);
            prepStmt = connTarget.prepareStatement(Sql_InsertToTarget);
            if (insertRows != null) {
                for (List<FieldValue> fieldValues : insertRows) {
                    int idx = 0;
                    for (FieldValue fieldValue : fieldValues) {
                        idx ++;
                        prepStmt = DBUtils.setPrepareStatement(appName, targetDbType, idx, fieldValue, connTarget, prepStmt);
                    }
                    prepStmt.addBatch();
                }
            }
            prepStmt.executeBatch();
            prepStmt.clearBatch();
            connTarget.commit();
            isSuccessToTarget = true;
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
            logger.info(appName + " 同步" + sourceTableName + "到" + targetTableName + "了" + insertRows.size() + "条" + System.currentTimeMillis());
        }
    }

    /**
     * 处理更新数据集合
     *
     * @param updateRows
     */
    private void processUpdateRows(List<List<FieldValue>> updateRows) {
        boolean isSuccessToTarget = false;
        Connection connTarget = null;
        PreparedStatement prepStmt = null;
        try {
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
            logger.info(appName + " 同步更新" + sourceTableName + "到" + targetTableName + "了" + updateRows.size() + "条" + System.currentTimeMillis());
        }
    }

    /**
     * 获取源表记录
     *
     * @return
     */
    private List<List<FieldValue>> selectFromSource(String startDate, String endDate, int start, int end) {
        List<List<FieldValue>> sourceValueLists = new ArrayList<List<FieldValue>>();
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
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
     * @return
     */
    private CompareRows listCompareTarget(List<List<FieldValue>> rows) {
        List<List<FieldValue>> insertRows = new ArrayList<>();
        List<List<FieldValue>> updateRows = new ArrayList<>();
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        try {
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
