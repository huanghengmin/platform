package com.hzih.db.entirely;

import com.hzih.db.entity.*;
import com.hzih.db.utils.DBUtils;
import com.hzih.jdbc.DataSourceUtil;
import com.inetec.common.config.nodes.Jdbc;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.io.InputStream;
import java.io.Reader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by Administrator on 15-10-29.
 */
public class EntirelyNormal extends Thread {

    private static final Logger logger = Logger.getLogger(EntirelyNormal.class);

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
    private String Sql_DeleteToSource;

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
            logger.error(appName+"初始化数据源出错"+e.getMessage(),e);
        }

        maxRecords = type.getMaxRecords();
        fields = table.getFieldValueList();
        String Sql_View_Source = "";
        String Sql_View_Target = "";
        String Sql_InsertToTarget_Values = "";
        String Sql_UpdateToTarget_Values = " ";
        String Sql_TargetSelect_Where = " ";
        String Sql_SourceSelect_Where = " ";
        int idx = 0;
        sourceTablePkFieldList = new ArrayList<FieldValue>();
        for (FieldValue field : fields) {
            if(field.isPk()) {
                sourceTablePkFieldList.add(field);
            }

            idx ++;
            if(idx < fields.size()) {
                Sql_View_Source += field.getFieldName() + ","; //源端字段列表
                Sql_View_Target += field.getDestField() + ","; //目标端字段列表
                Sql_InsertToTarget_Values += "?,"; //目标端插入动态值
                Sql_UpdateToTarget_Values += field.getDestField() +  " =? , "; //目标端更新动态值
            } else {
                Sql_View_Source += field.getFieldName();
                Sql_View_Target += field.getDestField();
                Sql_InsertToTarget_Values += "?";
                Sql_UpdateToTarget_Values += field.getDestField() +  " =? ";
            }
        }

        for(int i=0;i<sourceTablePkFieldList.size();i++){
            FieldValue field = sourceTablePkFieldList.get(i);
            Sql_TargetSelect_Where += field.getDestField()+"=?";
            Sql_SourceSelect_Where += field.getFieldName()+"=?";
            if(i!=sourceTablePkFieldList.size()-1){
                Sql_TargetSelect_Where+=",";
                Sql_SourceSelect_Where+=",";
            }
        }

        Sql_SelectCountSource = "SELECT COUNT(*) FROM " + sourceTableName ; //查询源端总记录条数

        Sql_SelectFromSource = DBUtils.createSql_SelectFromSource(appName, sourceDbType, Sql_View_Source, sourceTableName, null, 0);

//        Sql_SelectFromSource=  "SELECT * FROM ( SELECT "+Sql_View_Source+", ROWNUM RN " +"FROM ("+"SELECT " + Sql_View_Source + " FROM " + sourceTableName+")  WHERE ROWNUM <= ? ) WHERE RN >= ?";  //源表查询

        Sql_SelectFromTarget = "SELECT " + Sql_View_Target + " FROM " + targetTableName +" WHERE "+Sql_TargetSelect_Where ; //目标端查询语句需要加入where条件

        Sql_InsertToTarget = "INSERT INTO "+targetTableName+ " (" +Sql_View_Target + ") VALUES (" + Sql_InsertToTarget_Values +")";  //目标插入动态语句

        Sql_UpdateToTarget = "UPDATE "+targetTableName+ " SET " + Sql_UpdateToTarget_Values +" WHERE "+Sql_TargetSelect_Where ;   //目标更新动态语句

        Sql_DeleteToSource = "DELETE FROM "+sourceTableName+" WHERE "+Sql_SourceSelect_Where;//

        pkSize = sourceTablePkFieldList.size();//主键个数
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
                    int count = selectCountSource(); //总条数
                    int pages = count / maxRecords;
                    pages += (int)Math.ceil(count % maxRecords);
                    for ( int i = 0; i < pages; i += 1 ){
                        int start = i * maxRecords + 1;
                        int end = ( i+1 ) * maxRecords;
                        if(end>count){
                            end = count;
                        }
                        processBatch(start,end);
                    }
                    try {
                        Thread.sleep(1000 * type.getInterval());
                    } catch (InterruptedException ex) {
                        logger.error(appName+"同步线程休眠间隔出错"+ex.getMessage(), ex);
                    }
                    if(table.isOnlyOnce()){
                        break;
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
     */
    private void processBatch(int start,int end){
        List<List<FieldValue>> rows = selectFromSource(start,end);
        if(rows.size()>0) {
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
     * @return
     */
    private int selectCountSource(){
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try{
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
    private void processInsertRows(List<List<FieldValue>> insertRows) {
        boolean isSuccessToTarget = false;
        boolean isSuccessToSource = false;
        Connection connTarget = null;
        Connection connSource = null;
        PreparedStatement prepStmt = null;
        PreparedStatement sourcePrepStmt = null;
        try{
            if(table.isSourceDeleteAble()){
                connSource = dataSource.getConnection();
                connSource.setAutoCommit(false);
                sourcePrepStmt= connSource.prepareStatement(Sql_DeleteToSource);
            }
            connTarget = dataTarget.getConnection();
            connTarget.setAutoCommit(false);
            prepStmt = connTarget.prepareStatement(Sql_InsertToTarget);
            if(insertRows!=null){
                for(List<FieldValue> fieldValues:insertRows){
                    int idx = 0;
                    List<FieldValue> pkFieldValues = new ArrayList<FieldValue>();
                    for (FieldValue fieldValue : fieldValues){
                        idx ++;
                        if(fieldValue.isPk()){
                            pkFieldValues.add(fieldValue);
                        }
                        prepStmt = DBUtils.setPrepareStatement(appName,targetDbType,idx,fieldValue,connTarget,prepStmt);
                    }
                    idx = 0;
                    if(sourcePrepStmt!=null) {
                        for (int i = 0; i < pkFieldValues.size(); i++) {
                            FieldValue fieldValue = pkFieldValues.get(i);
                            idx ++;
                            sourcePrepStmt = DBUtils.setPrepareStatement(appName,sourceDbType,idx,fieldValue,connSource,sourcePrepStmt);
                        }
                        sourcePrepStmt.addBatch();
                    }
                    prepStmt.addBatch();
                }
            }
            prepStmt.executeBatch();
            prepStmt.clearBatch();
            connTarget.commit();
            isSuccessToTarget = true;
            if(table.isSourceDeleteAble()){
                sourcePrepStmt.executeBatch();
                sourcePrepStmt.clearBatch();
                connSource.commit();
                isSuccessToSource = true;
            }
        } catch (Exception e) {
            logger.error(appName+"目标端插入记录出错"+e.getMessage(), e);
            if(!isSuccessToTarget){
                try {
                    connTarget.rollback();
                } catch (SQLException e1) {
                    logger.error(appName+"数据回滚出错"+e1.getMessage(),e1);
                }
            }
            isSuccessToTarget = false;
            if(!isSuccessToSource){
                if(connSource!=null){
                    try {
                        connSource.rollback();
                    } catch (SQLException e1) {
                        logger.error(appName+"数据回滚出错"+e1.getMessage(),e1);
                    }
                }
            }
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
                    logger.error(appName+"设置数据自动提交出错"+e.getMessage(),e);
                }
            }
            closeJdbc(connTarget,prepStmt,null);
            closeJdbc(connSource,sourcePrepStmt,null);
        }
        if(isSuccessToTarget){
            logger.info(appName + " 同步" + sourceTableName + "到" + targetTableName + "了" + insertRows.size() + "条" + System.currentTimeMillis());
        }
        if(isSuccessToSource){
            logger.info(appName + " 删除" + sourceTableName+ "到" + targetTableName + "同步后记录" + insertRows.size() + "条" + System.currentTimeMillis());
        }
    }

    /**
     * 处理更新数据集合
     * @param updateRows
     */
    private void processUpdateRows(List<List<FieldValue>> updateRows) {
        boolean isSuccessToTarget = false;
        boolean isSuccessToSource = false;
        Connection connTarget = null;
        PreparedStatement prepStmt = null;
        Connection connSource = null;
        PreparedStatement sourcePrepStmt = null;
        try{
            if(table.isSourceDeleteAble()){
                connSource = dataSource.getConnection();
                connSource.setAutoCommit(false);
                sourcePrepStmt = connSource.prepareStatement(Sql_DeleteToSource);
            }
            connTarget = dataTarget.getConnection();
            connTarget.setAutoCommit(false);
            prepStmt = connTarget.prepareStatement(Sql_UpdateToTarget);
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
                    for(int i=0;i<pkFieldValues.size();i++){
                        FieldValue fieldValue = pkFieldValues.get(i);
                        idx ++;
                        prepStmt = DBUtils.setPrepareStatement(appName,targetDbType,idx,fieldValue,connTarget,prepStmt);
                        if(table.isSourceDeleteAble()){
                            sourcePrepStmt =  DBUtils.setPrepareStatement(appName,sourceDbType,
                                    idx,fieldValue,connSource,sourcePrepStmt);
                        }
                    }
                    prepStmt.addBatch();
                    if(table.isSourceDeleteAble()){
                        sourcePrepStmt.addBatch();
                    }
                }
                prepStmt.executeBatch();
                prepStmt.clearBatch();
                connTarget.commit();
                isSuccessToTarget = true;
                if(table.isSourceDeleteAble()){
                    sourcePrepStmt.executeBatch();
                    sourcePrepStmt.clearBatch();
                    connSource.commit();
                    isSuccessToSource = true;
                }
            }
        } catch (Exception e) {
            logger.error(appName+"更新目标端数据记录出错"+e.getMessage(), e);
            if(!isSuccessToTarget){
                try {
                    connTarget.rollback();
                } catch (SQLException e1) {
                    logger.error(appName+"数据回滚出错"+e1.getMessage(),e1);
                }
            }
            isSuccessToTarget = false;
            if(!isSuccessToSource){
                if(connSource!=null){
                    try {
                        connSource.rollback();
                    } catch (SQLException e1) {
                        logger.error(appName+"数据回滚出错"+e1.getMessage(),e1);
                    }
                }
            }
        } finally {
            if (connTarget != null) {
                try {
                    connTarget.setAutoCommit(true);
                } catch (SQLException e) {
                    logger.error(appName+"设置目标端数据自动提交出错"+e.getMessage(),e);
                }
            }
            if (connSource != null) {
                try {
                    connSource.setAutoCommit(true);
                } catch (SQLException e) {
                    logger.error(appName+"设置目标端数据自动提交出错"+e.getMessage(),e);
                }
            }
            closeJdbc(connTarget,prepStmt,null);
            closeJdbc(connSource,sourcePrepStmt,null);
        }
        if(isSuccessToTarget){
            logger.info(appName + " 同步更新"+sourceTableName+"到"+targetTableName+"了"+updateRows.size()+"条" + System.currentTimeMillis());
        }
        if(isSuccessToSource){
            logger.info(appName + " 同步更新后删除源端"+sourceTableName+"到"+targetTableName+"了"+updateRows.size()+"条" + System.currentTimeMillis());
        }
    }

    /**
     * 获取源表记录
     * @return
     */
    private List<List<FieldValue>> selectFromSource(int start,int end) {
        List<List<FieldValue>> sourceValueLists = new ArrayList<List<FieldValue>>();
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try{
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
     * @param rows
     * @return
     */
    private CompareRows listCompareTarget(List<List<FieldValue>> rows){
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
            closeJdbc(conn,preparedStatement,null);
            CompareRows compareRows = new CompareRows(insertRows,updateRows);
            return compareRows;
        } catch (SQLException e) {
            logger.error(appName+"对比目标端记录出错"+e.getMessage(),e);
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
