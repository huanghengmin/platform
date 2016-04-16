package com.hzih.db.twoway;

import com.hzih.db.entity.FieldValue;
import com.hzih.db.entity.Table;
import com.hzih.db.entity.TempRow;
import com.hzih.db.entity.Type;
import com.hzih.db.utils.DBUtils;
import com.hzih.jdbc.DataSourceUtil;
import com.inetec.common.config.nodes.Jdbc;
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
public class TwoWayRely extends Thread {
    private static final Logger logger = Logger.getLogger(TwoWayRely.class);
    
    private boolean isRun = false;
    private boolean isStop = false;
    private boolean isSourceToTarget = false;
    private Type type;
    private DataSource dataSource = null;
    private DataSource dataTarget = null;
    private String appName;
    private String Sql_SelectFromTemp;
    private String Sql_DeleteTempTable;
    private String Sql_UpdateStatusTemp;
    private Map<String,String> Sql_SelectFromSourceMap;
    private Map<String,String> Sql_InsertToTargetMap;
    private Map<String,String> Sql_UpdateToTargetMap;
    private Map<String,String> Sql_DeleteToTargetMap;
    private Map<String,List<FieldValue>> sourceTablePkFieldListMap;
    private Map<String,List<FieldValue>> fieldsMap;
    private Map<String,Table> tableMap;
    private Map<String,String> targetTableNameMap;

    private String sourceDbType;
    private String targetDbType;

    public void config(Jdbc sourceJdbc, Jdbc targetJdbc,boolean isSourceToTarget) {
        if(isSourceToTarget){
            this.sourceDbType = sourceJdbc.getDbType();
            this.targetDbType = targetJdbc.getDbType();
        } else {
            this.sourceDbType = targetJdbc.getDbType();
            this.targetDbType = sourceJdbc.getDbType();
        }
    }

    public void init(Type type,boolean isSourceToTarget) {
        this.type = type;
        this.appName = type.getAppName();
        this.isSourceToTarget = isSourceToTarget;
        try {
            if(isSourceToTarget){
                dataSource = DataSourceUtil.getDataSource(DataSourceUtil.DRUID_SOURCE);
                dataTarget = DataSourceUtil.getDataSource(DataSourceUtil.DRUID_TARGET);
            } else {
                dataSource = DataSourceUtil.getDataSource(DataSourceUtil.DRUID_TARGET);
                dataTarget = DataSourceUtil.getDataSource(DataSourceUtil.DRUID_SOURCE);
            }
        } catch (Exception e) {
            logger.error(appName + "建立数据源失败"+e.getMessage(),e);
        }

        int maxRecords = type.getMaxRecords();
        String tempTableName;
        if(isSourceToTarget){
            tempTableName = type.getSourceTempTable();
        }else {
            tempTableName = type.getTargetTempTable();
        }
        String statusTempTable = tempTableName + "_STATUS";
        if(maxRecords > 100) {
            maxRecords = 100;
            type.setMaxRecords(100);
        }

        Sql_SelectFromSourceMap = new HashMap<String,String>();
        Sql_InsertToTargetMap = new HashMap<String,String>();
        Sql_UpdateToTargetMap = new HashMap<String,String>();
        Sql_DeleteToTargetMap = new HashMap<String,String>();
        sourceTablePkFieldListMap = new HashMap<String,List<FieldValue>>();
        fieldsMap = new HashMap<String,List<FieldValue>>();
        tableMap = new HashMap<String,Table>();
        targetTableNameMap = new HashMap<String,String>();

        List<Table> tableList = type.getTableList();
        for (Table table : tableList) {
            String sourceTableName;
            String targetTableName;
            if(isSourceToTarget){
                sourceTableName = table.getSourceTableName();
                targetTableName = table.getTargetTableName();
            } else {
                sourceTableName = table.getTargetTableName();
                targetTableName = table.getSourceTableName();
            }
            List<FieldValue> fields = table.getFieldValueList();
            String Sql_View_Source = "";
            String Sql_View_Target = "";
            String Sql_InsertToTarget_Values = "";
            String Sql_UpdateToTarget_Values = " ";
            List<FieldValue> sourceTablePkFieldList = new ArrayList<FieldValue>();
            int idx = 0;
            for (FieldValue field : fields) {
                if(field.isPk()) {
                    sourceTablePkFieldList.add(field);
                }
                idx ++;
                if(idx < fields.size()) {
                    if(isSourceToTarget) {
                        Sql_View_Source += field.getFieldName() + ",";
                        Sql_View_Target += field.getDestField() + ",";
                        Sql_InsertToTarget_Values += "?,";
                        Sql_UpdateToTarget_Values += field.getDestField() +  " =? , ";
                    } else {
                        Sql_View_Source += field.getDestField() + ",";
                        Sql_View_Target += field.getFieldName() + ",";
                        Sql_InsertToTarget_Values += "?,";
                        Sql_UpdateToTarget_Values += field.getFieldName() +  " =? , ";
                    }
                } else {
                    if(isSourceToTarget){
                        Sql_View_Source += field.getFieldName();
                        Sql_View_Target += field.getDestField();
                        Sql_InsertToTarget_Values += "?";
                        Sql_UpdateToTarget_Values += field.getDestField() +  " =? ";
                    } else {
                        Sql_View_Source += field.getDestField();
                        Sql_View_Target += field.getFieldName();
                        Sql_InsertToTarget_Values += "?";
                        Sql_UpdateToTarget_Values += field.getFieldName() +  " =? ";
                    }
                }
            }

            /** 查询临时表语句 **/
            Sql_SelectFromTemp = DBUtils.createSql_SelectFromTemp(appName, sourceDbType, tempTableName, null, maxRecords);
//        Sql_SelectFromTemp = "select id,dbname,tablename, pks, op,op_time from " +
//                "( select * from "+tempTableName+" where id> 0 " +
//                "order by id asc) tmp where rownum <= " + maxRecords;

            /** 删除临时表语句 **/
            Sql_DeleteTempTable = "delete from " + tempTableName + " where id >= ? and id <= ?";

            /** 查询源表语句 还需根据实际情况加上查询条件 **/
            String Sql_SelectFromSource = "select " + Sql_View_Source + " from " + sourceTableName;
            /** 插入语句 **/
            String Sql_InsertToTarget = "insert into "+targetTableName+ " (" +Sql_View_Target + ") values (" + Sql_InsertToTarget_Values +")";
            /** 修改语句 **/
            sourceTablePkFieldListMap.put(sourceTableName,sourceTablePkFieldList);
            String Sql_UpdateToTarget_Where = createSqlUpdateToTargetWhere(sourceTableName);
            String Sql_UpdateToTarget = "update "+targetTableName+ " set " + Sql_UpdateToTarget_Values + Sql_UpdateToTarget_Where;
            /** 删除语句 **/
            String Sql_DeleteToTarget = "delete from "+targetTableName ;

            Sql_UpdateStatusTemp = "update " + statusTempTable + " set action_status = 1 where table_name = ";

            Sql_SelectFromSourceMap.put(sourceTableName,Sql_SelectFromSource);
            Sql_InsertToTargetMap.put(sourceTableName,Sql_InsertToTarget);
            Sql_UpdateToTargetMap.put(sourceTableName,Sql_UpdateToTarget);
            Sql_DeleteToTargetMap.put(sourceTableName,Sql_DeleteToTarget);
            fieldsMap.put(sourceTableName,fields);
            tableMap.put(sourceTableName,table);
            targetTableNameMap.put(sourceTableName,targetTableName);

            logger.info(appName + "应用"+(isSourceToTarget?"源到目标":"目标到源")+"同步,表"
                    +sourceTableName+"同步到表"+targetTableName+"配置加载成功.");
        }
    }
    public boolean isRun() {
        return isRun;
    }

    public void stopThread() {
        isStop = true;
    }

    /**
     //取临时表数据:所有表混合取
     //取真实数据
     //转换成IUD 三种sql语句
     //提交给目标端
     //删除临时表
     */
    @Override
    public void run() {
        isRun = true;
        while (isRun) {
            if(!isStop) {
                do{
                    try{
                        List<TempRow> tempRows = selectFromTempTable(Sql_SelectFromTemp);
                        if(tempRows.size()==0){ //临时表中没有数据,等待
                            try {
                                Thread.sleep(1000 * type.getInterval());
                            } catch (InterruptedException e) {
                            }
                            continue;
                        }
                        String action_temp = null;
                        String tableName_temp = null;
                        String action = null;
                        String tableName = null;
                        List<TempRow> processList = new ArrayList<TempRow>();
                        for (TempRow tempRow : tempRows) {
                            action = tempRow.getAct();
                            tableName = tempRow.getTableName();
                            if(action.equals(action_temp)
                                    && tableName.equals(tableName_temp)){
                                processList.add(tempRow);
                            } else {
                                if(action_temp != null
                                        && tableName_temp != null) {
                                    //处理这批数据
                                    processSourceTable(processList,action,tableName);
                                    processList = new ArrayList<TempRow>();
                                }
                                processList.add(tempRow);
                            }
                            action_temp = action;
                            tableName_temp = tableName;
                        }
                        processSourceTable(processList,action,tableName);
                    } catch (Exception e) {
                        logger.error(appName + "应用"+(isSourceToTarget?"源到目标":"目标到源")+"同步,错误"+e.getMessage(),e);
                    }
                } while (!isStop);
            } else {
                isRun = false;
            }
        }

    }

    
    /**
     * 查询临时表数据
     * @param Sql_SelectFromTemp
     * @return
     */
    private List<TempRow> selectFromTempTable(String Sql_SelectFromTemp) {
        List<TempRow> tempRows = new ArrayList<TempRow>();
        Connection conn = null;
        Statement statement = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            statement = conn.createStatement();
            rs = statement.executeQuery(Sql_SelectFromTemp);
            while (rs.next()) {
                long id = rs.getLong(1);
                String dbname = rs.getString(2);
                String tablename = rs.getString(3);
                String pks = rs.getString(4);
                String option = rs.getString(5);
                Timestamp optionTime = rs.getTimestamp(6);
                TempRow tempRow = new TempRow();
                tempRow.setId(id);
                tempRow.setDatabaseName(dbname);
                tempRow.setTableName(tablename);
                tempRow.setPks(pks);
                tempRow.setAct(option);
                tempRow.setActTime(optionTime);
                tempRows.add(tempRow);
            }
        } catch (Exception e) {
            logger.error(appName + "应用" + (isSourceToTarget ? "源到目标" : "目标到源") + "同步,获取临时表数据错误" + e.getMessage(), e);
        } finally {
            closeJdbc(conn, statement, rs);
        }
        return tempRows;
    }

    /**
     * 处理一批同表的相同操作的数据
     * @param processList 临时表值
     * @param action
     */
    private void processSourceTable(List<TempRow> processList, String action,String sourceTableName) {
        boolean isSuccessToTarget = false;
        long start = System.currentTimeMillis();
        String targetTableName = targetTableNameMap.get(sourceTableName);
        Connection connTarget = null;
        PreparedStatement prepStmt = null;
        Table table = tableMap.get(sourceTableName);
        try{
            connTarget = dataTarget.getConnection();
            connTarget.setAutoCommit(false);
            if("I".equalsIgnoreCase(action)) {
                String Sql_SelectFromSource_Where = createSqlSelectFromSourceWhere(processList,sourceTableName);
                List<List<FieldValue>> sourceValueLists = selectFromSource(Sql_SelectFromSource_Where,sourceTableName);
                String Sql_InsertToTarget = Sql_InsertToTargetMap.get(sourceTableName);
                prepStmt = connTarget.prepareStatement(Sql_InsertToTarget);
                int index = 0;
                for(List<FieldValue> sourceValueList : sourceValueLists) {
                    prepStmt = connTarget.prepareStatement(Sql_UpdateStatusTemp +"'"+ targetTableNameMap.get(sourceTableName) +"'");
                    prepStmt.executeUpdate();
                    prepStmt = connTarget.prepareStatement(Sql_InsertToTarget);
                    int idx = 0;
                    for (FieldValue fieldValue : sourceValueList) {
                        idx ++;
                        prepStmt = DBUtils.setPrepareStatement(appName,targetDbType,idx,fieldValue,connTarget,prepStmt);
                    }
                    prepStmt.execute();
                    connTarget.commit();
                }
            } else if("U".equalsIgnoreCase(action)) {
                if(!table.isTargetOnlyInsert()) {
                    String Sql_SelectFromSource_Where = createSqlSelectFromSourceWhere(processList,sourceTableName);
                    List<List<FieldValue>> sourceValueLists = selectFromSource(Sql_SelectFromSource_Where,sourceTableName);
                    String Sql_UpdateToTarget = Sql_UpdateToTargetMap.get(sourceTableName);
                    prepStmt = connTarget.prepareStatement(Sql_UpdateToTarget);
                    int index = 0;
                    for(List<FieldValue> sourceValueList : sourceValueLists){
                        prepStmt = connTarget.prepareStatement(Sql_UpdateStatusTemp +"'"+ targetTableNameMap.get(sourceTableName) +"'");
                        prepStmt.executeUpdate();
                        prepStmt = connTarget.prepareStatement(Sql_UpdateToTarget);
                        int idx = 0;
                        List<FieldValue> pkList = new ArrayList<FieldValue>();
                        for (FieldValue fieldValue : sourceValueList){
                            if(fieldValue.isPk()){
                                pkList.add(fieldValue);
                            }
                            idx ++;
                            prepStmt = DBUtils.setPrepareStatement(appName,targetDbType,idx, fieldValue, connTarget, prepStmt);
                        }
                        for(FieldValue fieldPk : pkList) {
                            idx ++;
                            prepStmt = DBUtils.setPrepareStatement(appName,targetDbType,idx, fieldPk, connTarget, prepStmt);
                        }
                        prepStmt.executeUpdate();
                        connTarget.commit();
                    }
                }
            } else if("D".equalsIgnoreCase(action)) {
                if(table.isTargetDeleteAble()){
                    String Sql_DeleteToTarget_Where = "";
                    for (TempRow tempRow : processList) {
                        String Sql_DeleteToTarget = Sql_DeleteToTargetMap.get(sourceTableName);
                        Sql_DeleteToTarget_Where = createSqlDeleteToTargetWhere(tempRow,sourceTableName);

                        prepStmt = connTarget.prepareStatement(Sql_UpdateStatusTemp + "'" + targetTableNameMap.get(sourceTableName) + "'");
                        prepStmt.executeUpdate();
                        prepStmt = connTarget.prepareStatement(Sql_DeleteToTarget + Sql_DeleteToTarget_Where);
                        prepStmt.executeUpdate();
                        connTarget.commit();
                    }
                }
            }
            isSuccessToTarget = true;
        } catch (Exception e) {
            logger.error(appName + "应用"+(isSourceToTarget?"源到目标":"目标到源")+"同步,同步表"+
                    sourceTableName+"到"+targetTableName+"操作数据错误"+e.getMessage(),e);
            try {
                connTarget.rollback();
            } catch (SQLException e1) {
            }
            isSuccessToTarget = false;
        } finally {
            if (connTarget != null) {
                try {
                    connTarget.setAutoCommit(true);
                } catch (SQLException e) {
                }
            }
            closeJdbc(connTarget,prepStmt,null);
        }
        //delete temptable
        if(isSuccessToTarget){
            deleteTempTable(processList);
        }
        long time = System.currentTimeMillis() - start;
        logger.info(appName + "应用"+(isSourceToTarget?"源到目标":"目标到源")+"同步,同步"+
                sourceTableName+"到"+targetTableName+"了"+processList.size()+"条,共计"+time+"毫秒");
    }

    /**
     * 组织查询源表时的条件语句
     * @param processList
     * @return
     */
    private String createSqlSelectFromSourceWhere(List<TempRow> processList,String sourceTableName) {
        String Sql_SelectFromSource_Where = "";
        List<FieldValue> sourceTablePkFieldList = sourceTablePkFieldListMap.get(sourceTableName);
        int pkSize = sourceTablePkFieldListMap.get(sourceTableName).size();
        if(pkSize == 1){
            Sql_SelectFromSource_Where += " where " + sourceTablePkFieldList.get(0).getFieldName() +
                    " in (";
            int idx = 0;
            for (TempRow tempRow : processList){
                idx ++;
                String pk = tempRow.getPks().split(";")[2];
                String fieldType = tempRow.getPks().split(";")[1];
                pk = DBUtils.createSql_Where_pk_Twoway(appName,sourceDbType,fieldType,pk);
                if(idx < processList.size()) {
                    Sql_SelectFromSource_Where += pk + ",";
                } else {
                    Sql_SelectFromSource_Where += pk + ")";
                }
            }
        } else {
            Sql_SelectFromSource_Where = " where ";
            int idx = 0;
            for (TempRow tempRow : processList){
                String pks = tempRow.getPks();
                String[] pkFields = pks.split(";")[0].split(",");
                String[] pkFieldTypes = pks.split(";")[1].split(",");
                String[] pkValues = pks.split(";")[2].split(",");
                Sql_SelectFromSource_Where += "(";
                for (int i = 0; i<pkSize;i++) {
                    String fieldType = pkFieldTypes[i];
                    String pkName = pkFields[i];
                    String pkStr = pkValues[i];
                    String pk = DBUtils.createSql_Where_pk_Twoway(appName,targetDbType,fieldType,pkStr);
                    Sql_SelectFromSource_Where += pkName + " = " + pk;
                    Sql_SelectFromSource_Where += " and ";
                }
                idx ++;
                if(idx < processList.size()) {
                    Sql_SelectFromSource_Where += " 1=1 ) or ";
                } else {
                    Sql_SelectFromSource_Where += " 1=1 )";
                }
            }
        }
        return Sql_SelectFromSource_Where;
    }


    /**
     * 通过临时表的值获取源表记录
     * @param Sql_SelectFromSource_Where
     * @return
     */
    private List<List<FieldValue>> selectFromSource(String Sql_SelectFromSource_Where,String sourceTableName) {
        List<List<FieldValue>> sourceValueLists = new ArrayList<List<FieldValue>>();
        Connection conn = null;
        Statement statement = null;
        ResultSet rs = null;
        String Sql_SelectFromSource = Sql_SelectFromSourceMap.get(sourceTableName);
        List<FieldValue> fields = fieldsMap.get(sourceTableName);
        try{
            conn = dataSource.getConnection();
            statement = conn.createStatement();
            rs = statement.executeQuery(Sql_SelectFromSource + Sql_SelectFromSource_Where);
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
            logger.error(appName + "应用"+(isSourceToTarget?"源到目标":"目标到源")+"同步,读取表"+sourceTableName+ "的记录失败,原因:" + e.getMessage(),e);
        } finally {
            closeJdbc(conn,statement,rs);
        }
        return sourceValueLists;
    }

    /**
     * 组织修改目标表时的条件语句
     * @return
     */
    private String createSqlUpdateToTargetWhere(String sourceTableName) {
        String Sql_UpdateToTarget_Where = " where ";
        List<FieldValue> sourceTablePkFieldList = sourceTablePkFieldListMap.get(sourceTableName);
        int idx = 0;
        for (FieldValue pkField : sourceTablePkFieldList){
            if(++idx < sourceTablePkFieldList.size()){
                Sql_UpdateToTarget_Where += pkField.getDestField() + " =? and ";
            } else {
                Sql_UpdateToTarget_Where += pkField.getDestField() + " =?";
            }
        }
        return Sql_UpdateToTarget_Where;
    }

    /**
     * 组织删除目标表时的条件语句 单条
     * @return
     */
    private String createSqlDeleteToTargetWhere(TempRow tempRow,String sourceTableName) {
        String Sql_DeleteToTarget_Where = "";
        String pks = tempRow.getPks();
        List<FieldValue> sourceTablePkFieldList = sourceTablePkFieldListMap.get(sourceTableName);
        int pkSize = sourceTablePkFieldListMap.get(sourceTableName).size();
        if(pkSize == 1) {
            FieldValue pkField = sourceTablePkFieldList.get(0);
            String fieldType = pkField.getDbType();
            String pkName = pkField.getDestField();
            String pkStr = pks.split(";")[2];
            String pk = DBUtils.createSql_Where_pk_Twoway(appName,targetDbType,fieldType,pkStr);
            Sql_DeleteToTarget_Where += " where " + pkName + " = " + pk;

        } else {//id1,id2;number,varchar2;2,bcd;
            String[] pkFields = pks.split(";")[0].split(",");
            String[] pkFieldTypes = pks.split(";")[1].split(",");
            String[] pkValues = pks.split(";")[2].split(",");
            Sql_DeleteToTarget_Where += " where ";
            for (int i = 0; i<pkSize;i++) {
                String fieldType = pkFieldTypes[i];
                String pkName = pkFields[i];
                String pkStr = pkValues[i];
                String pk = DBUtils.createSql_Where_pk_Twoway(appName,targetDbType,fieldType,pkStr);
                Sql_DeleteToTarget_Where +=  pkName + " = " + pk;
                Sql_DeleteToTarget_Where += " and ";
            }
            Sql_DeleteToTarget_Where += " 1=1";
        }
        return Sql_DeleteToTarget_Where;
    }

    /**
     * 删除临时表记录
     * @param processList
     */
    private void deleteTempTable(List<TempRow> processList) {
        Connection conn = null;
        PreparedStatement preStat = null;
        long min = processList.get(0).getId();
        long max = processList.get(processList.size()-1).getId();
//        String Sql_DeleteTempTable = "delete from " + tempTable + " where id >= "+min+" and id <= " + max;
        try{
            conn = dataSource.getConnection();
            preStat = conn.prepareStatement(Sql_DeleteTempTable);
            preStat.setLong(1,min);
            preStat.setLong(2,max);
            boolean isSuccess = preStat.execute();
//            logger.info(appName +"应用" +(isSourceToTarget?"源到目标":"目标到源")+ "同步,删除临时表记录成功");
        } catch (Exception e) {
            logger.error(appName +"应用" +(isSourceToTarget?"源到目标":"目标到源")+ "同步,删除临时表记录失败", e);
        } finally {
            closeJdbc(conn,preStat,null);
        }
    }
    

    private void closeJdbc(Connection conn, Statement statement, ResultSet rs) {
        if(rs!=null){
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(statement!=null){
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
