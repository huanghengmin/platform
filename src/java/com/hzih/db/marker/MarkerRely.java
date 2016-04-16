package com.hzih.db.marker;

import com.hzih.db.entity.*;
import com.hzih.db.utils.DBUtils;
import com.hzih.jdbc.DataSourceUtil;
import com.inetec.common.config.nodes.Jdbc;
import com.sun.xml.internal.ws.api.ha.StickyFeature;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.io.InputStream;
import java.io.Reader;
import java.sql.*;
import java.util.*;


/**
 * Created by Administrator on 15-10-29.
 */
public class MarkerRely extends Thread {

    private static final Logger logger = Logger.getLogger(MarkerRely.class);

    private boolean isRun = false;
    private boolean isStop = false;
    private Type type;
    private DataSource dataSource = null;
    private DataSource dataTarget = null;
    private String appName;

    private int maxRecords;
    private Map<String,String> Sql_SelectCountSourceMap;
    private Map<String,String> Sql_SelectFromSourceMap;
    private Map<String,String> Sql_SelectFromTargetMap;
    private Map<String,String> Sql_InsertToTargetMap;
    private Map<String,String> Sql_UpdateToTargetMap;
    private Map<String,String> Sql_UpdateToSourceMap;
    private Map<String,List<FieldValue>> sourceTablePkFieldListMap;
    private Map<String,List<FieldValue>> fieldsMap;
    private Map<String,Table> tableMap;
    private Map<String,String> targetTableNameMap;

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
            logger.error(appName+"初始化数据源出错"+e.getMessage(),e);
        }
        maxRecords = type.getMaxRecords();

        Sql_SelectCountSourceMap = new HashMap<String,String>();
        Sql_SelectFromSourceMap = new HashMap<String,String>();
        Sql_SelectFromTargetMap = new HashMap<String,String>();
        Sql_InsertToTargetMap = new HashMap<String,String>();
        Sql_UpdateToTargetMap = new HashMap<String,String>();
        Sql_UpdateToSourceMap = new HashMap<String,String>();
        sourceTablePkFieldListMap = new HashMap<String,List<FieldValue>>();
        fieldsMap = new HashMap<String,List<FieldValue>>();
        tableMap = new HashMap<String,Table>();
        targetTableNameMap = new HashMap<String,String>();

        List<Table> tableList = type.getTableList();
        for (Table table : tableList){
            String sourceTableName = table.getSourceTableName();
            String targetTableName = table.getTargetTableName();

            List<FieldValue> fields = table.getFieldValueList();
            int syncFlagValue = 1;
            int flagValue = 0;
            String flag = table.getFlagName();
            String Sql_View_Source = "";
            String Sql_View_Target = "";
            String Sql_InsertToTarget_Values = "";
            String Sql_UpdateToTarget_Values = " ";
            String Sql_TargetSelect_Where = " ";
            String Sql_SourceSelect_Where = " ";
            int idx = 0;
            List<FieldValue> sourceTablePkFieldList = new ArrayList<FieldValue>();
            for (FieldValue field : fields) {
                if(field.isPk()) {
                    sourceTablePkFieldList.add(field);
                }
                idx ++;
                if(idx < fields.size()) {
                    Sql_View_Source += field.getFieldName() + ",";
                    Sql_View_Target += field.getDestField() + ",";
                    Sql_InsertToTarget_Values += "?,";
                    Sql_UpdateToTarget_Values += field.getDestField() +  " =? , ";
                } else {
                    Sql_View_Source += field.getFieldName();
                    Sql_View_Target += field.getDestField();
                    Sql_InsertToTarget_Values += "?";
                    Sql_UpdateToTarget_Values += field.getDestField() +  " =? ";
                }
            }

            for (int y = 0; y < sourceTablePkFieldList.size();y++) {
                FieldValue field = sourceTablePkFieldList.get(y);
                Sql_TargetSelect_Where += field.getDestField() + "=?";
                Sql_SourceSelect_Where += field.getFieldName()+"=?";
                if (y != sourceTablePkFieldList.size() - 1) {
                    Sql_TargetSelect_Where += ",";
                    Sql_SourceSelect_Where += ",";
                }
            }

            String Sql_SelectCountSource = "SELECT COUNT(*) FROM " + sourceTableName +" WHERE "+flag+"="+syncFlagValue; //查询源端总记录条数

            String Sql_SelectFromSource = DBUtils.createSql_SelectFromSource(appName,sourceDbType,Sql_View_Source,sourceTableName,flag, syncFlagValue);

//            String Sql_SelectFromSource =  "SELECT * FROM ( SELECT "+Sql_View_Source+", ROWNUM RN " +"FROM ("+"SELECT " + Sql_View_Source + " FROM " + sourceTableName+" WHERE "+flag+"="+syncFlagValue+")  WHERE ROWNUM <= ? ) WHERE RN >= ?";  //源表查询

            String Sql_SelectFromTarget = "SELECT " + Sql_View_Target + " FROM " + targetTableName + " WHERE " + Sql_TargetSelect_Where; //目标端查询语句需要加入where条件

            String Sql_InsertToTarget = "INSERT INTO " + targetTableName + " (" + Sql_View_Target + ") VALUES (" + Sql_InsertToTarget_Values + ")";  //目标插入动态语句

            String Sql_UpdateToTarget = "UPDATE " + targetTableName + " SET " + Sql_UpdateToTarget_Values + " WHERE " + Sql_TargetSelect_Where;   //目标更新动态语句

            String Sql_UpdateToSource = "UPDATE " + sourceTableName + " SET " + flag + "="+flagValue+" WHERE " + Sql_SourceSelect_Where;   //目标更新动态语句

            Sql_SelectCountSourceMap.put(sourceTableName,Sql_SelectCountSource);
            Sql_SelectFromSourceMap.put(sourceTableName,Sql_SelectFromSource);
            Sql_SelectFromTargetMap.put(sourceTableName,Sql_SelectFromTarget);
            Sql_InsertToTargetMap.put(sourceTableName,Sql_InsertToTarget);
            Sql_UpdateToTargetMap.put(sourceTableName,Sql_UpdateToTarget);
            Sql_UpdateToSourceMap.put(sourceTableName,Sql_UpdateToSource);
            sourceTablePkFieldListMap.put(sourceTableName,sourceTablePkFieldList);
            fieldsMap.put(sourceTableName,fields);
            tableMap.put(sourceTableName,table);
            targetTableNameMap.put(sourceTableName,targetTableName);

        }
    }

    public boolean isRun() {
        return isRun;
    }

    public void stopThread() {
        isStop = true;
    }

    /**
     //取真实数据
     //转换成IUD 三种sql语句
     //提交给目标端
     */
    @Override
    public void run() {
        isRun = true;
        while (isRun) {
            if (!isStop) {
                do {
                    for (Table table: type.getTableList()) {
                        String sourceTableName = table.getSourceTableName();
                        int count = selectCountSource(sourceTableName); //总条数
                        int pages = count / maxRecords;
                        pages += (int)Math.ceil(count % maxRecords);
                        for ( int i = 0; i <= pages; i += 1 ){
                            int start = i*maxRecords+1;
                            int end = (i+1)*maxRecords;
                            if(end>count){
                                end = count;
                            }
                            processBatch(start,end,sourceTableName);
                        }
                    }
                    try {
                        Thread.sleep(1000 * type.getInterval());
                    } catch (InterruptedException e) {
                        logger.error(appName+"数据同步休眠间隔出错"+e.getMessage(),e);
                    }
                } while (!isStop);
            } else {
                isRun = false;
            }
        }
    }


    /**
     *
     * @param start
     * @param end
     * @param sourceTableName
     */
    private void processBatch(int start, int end, String sourceTableName){
        List<List<FieldValue>> rows = selectFromSource(start,end,sourceTableName);
        if(rows.size()>0) {
            Table table = tableMap.get(sourceTableName);
            if (table.isTargetOnlyInsert()) {
                processInsertRows(rows,sourceTableName);
            } else {
                CompareRows compareRows = listCompareTarget(rows,sourceTableName);
                if (compareRows != null) {
                    List<List<FieldValue>> insertRows = compareRows.getInsertRows();
                    List<List<FieldValue>> updateRows = compareRows.getUpdateRows();
                    if(insertRows.size()>0) {
                        processInsertRows(insertRows,sourceTableName);
                    }
                    if(updateRows.size()>0) {
                        processUpdateRows(updateRows,sourceTableName);
                    }
                }
            }
        }
    }

    /**
     * 查询源端总记录条数
     * @return
     * @param sourceTableName
     */
    private int selectCountSource(String sourceTableName){
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try{
            String Sql_SelectCountSource = Sql_SelectCountSourceMap.get(sourceTableName);
            conn = dataSource.getConnection();
            preparedStatement = conn.prepareStatement(Sql_SelectCountSource);
            rs = preparedStatement.executeQuery();
            int rowCount = 0;
            if(rs.next()){
                rowCount = rs.getInt(1);
                return rowCount;
            }
        } catch (Exception e) {
            logger.error("应用" + appName + "读取表"+sourceTableName+ "的记录总数失败,原因:" + e.getMessage(),e);
            return 0;
        } finally {
            closeJdbc(conn,preparedStatement,rs);
        }
        return 0;
    }

    /**
     * 处理插入数据集合
     */
    private void processInsertRows(List<List<FieldValue>> insertRows, String sourceTableName) {
        boolean isSuccessToTarget = false;
        boolean isSuccessToSource = false;
        Connection connTarget = null;
        Connection connSource = null;
        PreparedStatement prepStmt = null;
        PreparedStatement sourcePrepStmt = null;
        try{
            String Sql_InsertToTarget = Sql_InsertToTargetMap.get(sourceTableName);
            String Sql_UpdateToSource = Sql_UpdateToSourceMap.get(sourceTableName);
            connTarget = dataTarget.getConnection();
            connSource = dataSource.getConnection();
            connSource.setAutoCommit(false);
            connTarget.setAutoCommit(false);
            prepStmt = connTarget.prepareStatement(Sql_InsertToTarget);
            sourcePrepStmt = connSource.prepareStatement(Sql_UpdateToSource);
            if(insertRows!=null){
                for(List<FieldValue> fieldValues:insertRows){
                    int idx = 0;
                    List<FieldValue> pkFieldValues = new ArrayList<>();
                    for (FieldValue fieldValue : fieldValues){
                        if(fieldValue.isPk()){
                            pkFieldValues.add(fieldValue);
                        }
                        idx ++;
                        prepStmt = DBUtils.setPrepareStatement(appName,targetDbType,idx,fieldValue,connTarget,prepStmt);
                    }
                    idx = 0;
                    for(int i=0;i<pkFieldValues.size();i++){
                        FieldValue fieldValue = pkFieldValues.get(i);
                        idx ++;
                        sourcePrepStmt = DBUtils.setPrepareStatement(appName,sourceDbType,idx, fieldValue, connSource, sourcePrepStmt);
                    }
                    sourcePrepStmt.addBatch();
                    prepStmt.addBatch();
                }
                prepStmt.executeBatch();
                sourcePrepStmt.executeBatch();
                prepStmt.clearBatch();
                sourcePrepStmt.clearBatch();
                connTarget.commit();
                connSource.commit();
                isSuccessToTarget = true;
                isSuccessToSource = true;
            }

        } catch (Exception e) {
            logger.error(appName+"目标端插入记录失败"+e.getMessage(), e);
            try {
                connTarget.rollback();
                connSource.rollback();
            } catch (SQLException e1) {
                logger.error(appName+"数据回滚操作出错"+e1.getMessage(),e1);
            }
            isSuccessToTarget = false;
        } finally {
            if (connTarget != null) {
                try {
                    connTarget.setAutoCommit(true);
                } catch (SQLException e) {
                    logger.error(appName+"设置数据自动提交出错"+e.getMessage(),e);
                }
            }
            if (connSource != null) {
                try {
                    connSource.setAutoCommit(true);
                } catch (SQLException e) {
                    logger.error(appName+"设置源端数据自动提交出错"+e.getMessage(),e);
                }
            }
            closeJdbc(connTarget,prepStmt,null);
            closeJdbc(connSource,sourcePrepStmt,null);
        }
        if(isSuccessToTarget){
            String targetTableName = targetTableNameMap.get(sourceTableName);
            logger.info(appName + " 同步" + sourceTableName + "到" + targetTableName + "了" + insertRows.size() + "条" + System.currentTimeMillis());
        }

        if(isSuccessToSource){
            String targetTableName = targetTableNameMap.get(sourceTableName);
            logger.info(appName + " 更新源端标记" + sourceTableName + "到" + targetTableName + "了" + insertRows.size() + "条" + System.currentTimeMillis());
        }
    }

    /**
     * 处理更新数据集合
     * @param updateRows
     * @param sourceTableName
     */
    private void processUpdateRows(List<List<FieldValue>> updateRows, String sourceTableName) {
        boolean isSuccessToTarget = false;
        boolean isSuccessToSource = false;
        Connection connTarget = null;
        Connection connSource = null;
        PreparedStatement prepStmt = null;
        PreparedStatement sourcePrepStmt = null;
        try{
            String Sql_UpdateToTarget = Sql_UpdateToTargetMap.get(sourceTableName);
            String Sql_UpdateToSource = Sql_UpdateToSourceMap.get(sourceTableName);
            connTarget = dataTarget.getConnection();
            connSource = dataSource.getConnection();
            connTarget.setAutoCommit(false);
            connSource.setAutoCommit(false);
            prepStmt = connTarget.prepareStatement(Sql_UpdateToTarget);
            sourcePrepStmt = connSource.prepareStatement(Sql_UpdateToSource);
            if(updateRows!=null){
                for(List<FieldValue> fieldValues:updateRows){
                    int idx = 0;
                    List<FieldValue> pkFieldValues = new ArrayList<>();
                    for (FieldValue fieldValue : fieldValues){
                        if(fieldValue.isPk()){
                            pkFieldValues.add(fieldValue);
                        }
                        idx ++;
                        prepStmt = DBUtils.setPrepareStatement(appName,targetDbType,idx,fieldValue,connTarget,prepStmt);
                    }
                    idx = 0;
                    for(int i=0;i<pkFieldValues.size();i++){
                        FieldValue fieldValue = pkFieldValues.get(i);
                        idx ++;
                        sourcePrepStmt = DBUtils.setPrepareStatement(appName,sourceDbType,idx, fieldValue, connSource, sourcePrepStmt);
                    }
                    prepStmt.addBatch();
                    sourcePrepStmt.addBatch();
                }
                prepStmt.executeBatch();
                sourcePrepStmt.executeBatch();
                prepStmt.clearBatch();
                sourcePrepStmt.clearBatch();
                connTarget.commit();
                connSource.commit();
                isSuccessToTarget = true;
                isSuccessToSource = true;
            }
        } catch (Exception e) {
            logger.error(appName+"更新目标端数据记录出错"+e.getMessage(), e);
            try {
                connTarget.rollback();
                connSource.rollback();
            } catch (SQLException e1) {
                logger.error(appName+"数据回滚出错"+e1.getMessage(),e1);
            }
            isSuccessToTarget = false;
            isSuccessToSource = false;
        } finally {
            if (connTarget != null) {
                try {
                    connTarget.setAutoCommit(true);
                } catch (SQLException e) {
                    logger.error(appName+"设置数据自动提交出错"+e.getMessage(),e);
                }
            }
            if (connSource != null) {
                try {
                    connSource.setAutoCommit(true);
                } catch (SQLException e) {
                    logger.error(appName+"设置源端数据自动提交出错"+e.getMessage(),e);
                }
            }
            closeJdbc(connTarget,prepStmt,null);
            closeJdbc(connSource,sourcePrepStmt,null);
        }
        if(isSuccessToTarget){
            String targetTableName = targetTableNameMap.get(sourceTableName);
            logger.info(appName + " 同步更新" + sourceTableName + "到" + targetTableName + "了" + updateRows.size() + "条" + System.currentTimeMillis());
        }

        if(isSuccessToSource){
            String targetTableName = targetTableNameMap.get(sourceTableName);
            logger.info(appName + " 更新源端标记录"+sourceTableName+"到"+targetTableName+"了"+updateRows.size()+"条" + System.currentTimeMillis());
        }
    }

    /**
     * 获取源表记录
     * @return
     */
    private List<List<FieldValue>> selectFromSource(int start, int end, String sourceTableName) {
        List<List<FieldValue>> sourceValueLists = new ArrayList<List<FieldValue>>();
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try{
            String Sql_SelectFromSource = Sql_SelectFromSourceMap.get(sourceTableName);
            List<FieldValue> fields = fieldsMap.get(sourceTableName);
            conn = dataSource.getConnection();
            preparedStatement = conn.prepareStatement(Sql_SelectFromSource);
            preparedStatement.setInt(1,end);
            preparedStatement.setInt(2,start);
            rs = preparedStatement.executeQuery();
            while (rs.next()){
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
            logger.error("应用" + appName + "读取表"+sourceTableName+ "的记录失败,原因:" + e.getMessage(),e);
        } finally {
            closeJdbc(conn,preparedStatement,rs);
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
    private CompareRows listCompareTarget(List<List<FieldValue>> rows, String sourceTableName){
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
            closeJdbc(conn,preparedStatement,null);
            CompareRows compareRows = new CompareRows(insertRows,updateRows);
            return compareRows;
        } catch (SQLException e) {
            logger.error(appName+"遍历对比目标端数据出错"+e.getMessage(),e);
            return null;
        }
    }

    /**
     *
     * @param conn
     * @param statement
     * @param rs
     */
    private void closeJdbc(Connection conn, Statement statement, ResultSet rs) {
        if(rs!=null){
            try {
                rs.close();
            } catch (SQLException e) {
                logger.error(appName+"关闭数据库结果集出错"+e.getMessage(), e);
            }
        }
        if(statement!=null){
            try {
                statement.close();
            } catch (SQLException e) {
                logger.error(appName+"关闭数据库会话出错"+e.getMessage(), e);
            }
        }
        if(conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error(appName+"关闭数据库连接出错"+e.getMessage(),e);
            }
        }
    }
}
