package com.hzih.jdbc.oracle;

import com.inetec.common.config.nodes.Field;
import com.inetec.common.config.nodes.Jdbc;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 15-11-5.
 */
public class OracleInternal {
    private final static Logger logger = Logger.getLogger(OracleInternal.class);

    /**
     * 创建临时表
     * @param appName
     * @param tempTableName
     * @param jdbc
     * @return
     */
    public static boolean createSingleTempTable(String appName,String tempTableName,Jdbc jdbc){
        boolean isSuccess = false;
        String owner = jdbc.getDbOwner();
        String sequenceName = "ICHANGE_SEQUENCE_" + appName.toUpperCase();
        String Sql_Create_TempTable = "create table "+owner+"."+tempTableName+"(" +
                "ID NUMBER(10) not null," +
                "DBNAME VARCHAR2(255) not null," +
                "TABLENAME VARCHAR2(255) not null," +
                "PKS VARCHAR2(4000) not null," +
                "OP VARCHAR2(2) not null," +
                "OP_TIME DATE default SYSDATE," +
                "CONSTRAINT "+owner+"_"+tempTableName+"_ID_PK PRIMARY KEY (ID))";
        String Sql_Create_Sequence = "CREATE SEQUENCE "+owner+"."+sequenceName+" INCREMENT BY 1 START WITH 1 MAXVALUE 1.0E28 MINVALUE 1";

        Connection conn = null;
        PreparedStatement preStat = null;
        try {
            DataSource dataSource = DataSourceUtil.getDataSource(appName, jdbc);
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            preStat = conn.prepareStatement(Sql_Create_TempTable);
            preStat.execute();
            preStat = conn.prepareStatement(Sql_Create_Sequence);
            preStat.execute();
            conn.commit();
            isSuccess = true;
        } catch (Exception e) {
            logger.error("应用"+appName+"创建临时表错误"+e.getMessage(),e);
            try {
                conn.rollback();
            } catch (SQLException e1) {
            }
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                }
            }
            closeJdbc(conn,preStat,null);
        }
        return isSuccess;
    }

    public static boolean createTwoWayTempTable(String appName,String tempTableName,String tempStatusTableName,Jdbc jdbc){
        boolean isSuccess = false;
        String owner = jdbc.getDbOwner();
        String sequenceName = "ICHANGE_SEQUENCE_" + appName.toUpperCase();
        String Sql_Create_TempTable = "create table "+owner+"."+tempTableName+"(" +
                "ID NUMBER(10) not null," +
                "DBNAME VARCHAR2(255) not null," +
                "TABLENAME VARCHAR2(255) not null," +
                "PKS VARCHAR2(4000) not null," +
                "OP VARCHAR2(2) not null," +
                "OP_TIME DATE DEFAULT SYSDATE," +
                "CONSTRAINT "+owner+"_"+tempTableName+"_ID_PK PRIMARY KEY (ID))";
        String Sql_Create_Sequence = "CREATE SEQUENCE "+owner+"."+sequenceName+" INCREMENT BY 1 START WITH 1 MAXVALUE 1.0E28 MINVALUE 1";

        String Sql_Create_TempStatusTable = "create table "+owner+"."+tempStatusTableName+"(" +
                "TABLE_NAME VARCHAR2(255) not null," +
                "ACTION_STATUS NUMBER(1) DEFAULT 0," +
                "CONSTRAINT "+owner+"_"+tempStatusTableName+"_PK PRIMARY KEY (TABLE_NAME))";

        Connection conn = null;
        PreparedStatement preStat = null;
        try {
            DataSource dataSource = DataSourceUtil.getDataSource(appName,jdbc);
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            preStat = conn.prepareStatement(Sql_Create_Sequence);
            preStat.execute();
            preStat = conn.prepareStatement(Sql_Create_TempTable);
            preStat.execute();
            preStat = conn.prepareStatement(Sql_Create_TempStatusTable);
            preStat.execute();
            conn.commit();
            isSuccess = true;
        } catch (Exception e) {
            logger.error("应用"+appName+"创建临时表错误"+e.getMessage(),e);
            try {
                conn.rollback();
            } catch (SQLException e1) {
            }
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                }
            }
            closeJdbc(conn,preStat,null);
        }
        return isSuccess;
    }

    /**
     * 删除临时表
     * @param appName
     * @param tempTableName
     * @param jdbc
     * @return
     */
    public static boolean deleteSingleTempTable(String appName,String tempTableName,Jdbc jdbc){
        boolean isSuccess = false;
        String owner = jdbc.getDbOwner();
        String sequenceName = "ICHANGE_SEQUENCE_" + appName.toUpperCase();
        String Sql_Drop_TempTable = "drop table "+owner+"."+tempTableName;
        String Sql_Drop_Sequence = "drop SEQUENCE "+owner+"."+sequenceName;

        Connection conn = null;
        PreparedStatement preStat = null;
        try {
            DataSource dataSource = DataSourceUtil.getDataSource(appName,jdbc);
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            preStat = conn.prepareStatement(Sql_Drop_Sequence);
            preStat.execute();
            preStat = conn.prepareStatement(Sql_Drop_TempTable);
            preStat.execute();
            conn.commit();
            isSuccess = true;
        } catch (Exception e) {
            logger.error("应用"+appName+"删除临时表错误"+e.getMessage(),e);
            try {
                conn.rollback();
            } catch (SQLException e1) {
            }
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                }
            }
            closeJdbc(conn,preStat,null);
        }
        return isSuccess;
    }
    public static boolean deleteTwoWayTempTable(String appName,String tempTableName,String tempStatusTableName,Jdbc jdbc){
        boolean isSuccess = false;
        String owner = jdbc.getDbOwner();
        String sequenceName = "ICHANGE_SEQUENCE_" + appName.toUpperCase();
        String Sql_Drop_TempTable = "drop table "+owner+"."+tempTableName;
        String Sql_Drop_TempStatusTable = "drop table "+owner+"."+tempStatusTableName;
        String Sql_Drop_Sequence = "drop SEQUENCE "+owner+"."+sequenceName;

        Connection conn = null;
        PreparedStatement preStat = null;
        try {
            DataSource dataSource = DataSourceUtil.getDataSource(appName,jdbc);
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            preStat = conn.prepareStatement(Sql_Drop_Sequence);
            preStat.execute();
            preStat = conn.prepareStatement(Sql_Drop_TempTable);
            preStat.execute();
            preStat = conn.prepareStatement(Sql_Drop_TempStatusTable);
            preStat.execute();
            conn.commit();
            isSuccess = true;
        } catch (Exception e) {
            logger.error("应用"+appName+"删除临时表错误"+e.getMessage(),e);
            try {
                conn.rollback();
            } catch (SQLException e1) {
            }
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                }
            }
            closeJdbc(conn,preStat,null);
        }
        return isSuccess;
    }

    /**
     * 创建单向同步的触发器
     * @param appName
     * @param tempTableName
     * @param triggers  <tableName,<<I,I>,<U,U>,<D,D>>>
     * @param tableAndFields <tableName,<pkList>>
     * @param jdbc
     * @return   <tableName,<<I,name>,<U,name>,<D,name>>>
     */
    public static Map<String,Map<String,String>> createSingleTriggers(String appName,String tempTableName,
                                                                      Map<String,Map<String,String>> triggers,
                                                   Map<TableBean,List<Field>> tableAndFields,Jdbc jdbc){
        String owner = jdbc.getDbOwner().toUpperCase();
        String sequenceName = "ICHANGE_SEQUENCE_" + appName.toUpperCase();
        Map<String,Map<String,String>> returnTriggers = new HashMap<String,Map<String,String>>();
        for (Map.Entry<TableBean,List<Field>> key: tableAndFields.entrySet()) {
            TableBean tableBean = key.getKey();
            String tableName = tableBean.getTableName();
            List<Field> pkList = key.getValue();
            Map<String,String> trigger = triggers.get(tableName);//触发器个数: I,I U,U D,D
            String newpatterns = "";   //for add,update trigger
            String oldpatterns = "";   //for delete trigger
            String pkNames = "";
            String pkTypes = "";
            int k = 0;
            for (Field field : pkList){
                if(k > 0){
                    newpatterns = newpatterns + "||','||";
                    oldpatterns = oldpatterns + "||','||";
                }
                k++;
                if("date".equalsIgnoreCase(field.getJdbcType()) || "date".equalsIgnoreCase(field.getDbType())){
                    newpatterns = newpatterns + "TO_CHAR(:new." + field.getFieldName() + ",'YYYY-MM-DD HH24:MI:SS')";
                    oldpatterns = oldpatterns + "TO_CHAR(:old." + field.getFieldName() + ",'YYYY-MM-DD HH24:MI:SS')";
                } else if("timestamp".equalsIgnoreCase(field.getJdbcType())){
                    newpatterns = newpatterns + "TO_CHAR(:new." + field.getFieldName() + ",'YYYY-MM-DD HH24:MI:SS.FF')";
                    oldpatterns = oldpatterns + "TO_CHAR(:old." + field.getFieldName() + ",'YYYY-MM-DD HH24:MI:SS.FF')";
                } else{
                    newpatterns = newpatterns + "TO_CHAR(:new." + field.getFieldName() + ")";
                    oldpatterns = oldpatterns + "TO_CHAR(:old." + field.getFieldName() + ")";
                }
                if(pkNames.length() == 0){
                    pkNames = field.getFieldName();
                }else{
                    pkNames = pkNames + "," + field.getFieldName();
                }
                if(pkTypes.length() == 0){
                    pkTypes = field.getDbType();
                }else {
                    pkTypes = pkTypes + "," + field.getDbType();
                }
            }
            
            String select_I = newpatterns;//TO_CHAR(:new.ID)||','||TO_CHAR(:new.TEST1)
            String select_U = newpatterns;//TO_CHAR(:old.ID)||','||TO_CHAR(:old.TEST1)
            String select_D = new String(oldpatterns);//TO_CHAR(:old.ID)||','||TO_CHAR(:old.TEST1)
            String pkStr = pkNames +";" + pkTypes;
            int hashCode = Math.abs((appName + tableName + tempTableName).hashCode());
            String triggerName = "ICHANGE_"+ hashCode +"";
            String insteadOf = "";
            if(tableBean.isView()){
                insteadOf = "instead of";
            } else {
                insteadOf = "after";
            }
            String Sql_Single_Create_Trigger_Insert =
                    "create or replace trigger "+triggerName+"_I "+insteadOf+" insert on "+owner+"."+tableName+" for each row " +
                    "DECLARE strVals varchar2(2550); " +
                    "begin " +
                    "select ("+select_I+") INTO strVals from dual; " +
                    "insert into "+owner+"."+tempTableName+" (id,dbname,tablename,pks,op) " +
                    "values ("+owner+"."+sequenceName+".nextval,'"+owner+"','"+tableName.toUpperCase()+"','"+pkStr+";'||strVals||';','I');" +
                    "end;";
            String Sql_Single_Create_Trigger_Update =
                    "create or replace trigger "+triggerName+"_U "+insteadOf+" update on "+owner+"."+tableName+" for each row " +
                    "DECLARE strVals varchar2(2550); " +
                    "begin " +
                    "select ("+select_U+") INTO strVals from dual; " +
                    "insert into "+owner+"."+tempTableName+" (id,dbname,tablename,pks,op) " +
                    "values ("+owner+"."+sequenceName+".nextval,'"+owner+"','"+tableName.toUpperCase()+"','"+pkStr+";'||strVals||';','U');" +
                    "end;";
//            select_D = "123";
            String Sql_Single_Create_Trigger_Delete =
                    "create or replace trigger "+triggerName+"_D "+insteadOf+" delete on "+owner+"."+tableName+" for each row " +
                    "DECLARE strVals varchar2(2550); " +
                    "begin " +
                    "select ("+select_D+") INTO strVals from dual; " +
                    "insert into "+owner+"."+tempTableName+" (id,dbname,tablename,pks,op) " +
                    "values ("+owner+"."+sequenceName+".nextval,'"+owner+"','"+tableName.toUpperCase()+"','"+pkStr+";'||strVals||';','D');" +
                    "end;";
            Map<String,String> returnTrigger = new HashMap<String,String>();
            Connection conn = null;
//            PreparedStatement preStat = null;
            Statement statement = null;
            try {
                DataSource dataSource = DataSourceUtil.getDataSource(appName,jdbc);
                conn = dataSource.getConnection();
                conn.setAutoCommit(false);
                if(trigger.get("I")!=null){
                    statement = conn.createStatement();
                    statement.execute(Sql_Single_Create_Trigger_Insert);
                    returnTrigger.put("I",triggerName+"_I");
                }
                if(trigger.get("U")!=null){
                    statement = conn.createStatement();
                    statement.execute(Sql_Single_Create_Trigger_Update);
                    returnTrigger.put("U",triggerName+"_U");
                }
                if(trigger.get("D")!=null){
                    statement = conn.createStatement();
                    statement.execute(Sql_Single_Create_Trigger_Delete);
                    returnTrigger.put("D",triggerName+"_D");
                }
                conn.commit();
                returnTriggers.put(tableName,returnTrigger);
            } catch (Exception e) {
                logger.error("应用"+appName+"创建表"+tableName+"上的触发器错误"+e.getMessage(),e);
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                }
            } finally {
                if (conn != null) {
                    try {
                        conn.setAutoCommit(true);
                    } catch (SQLException e) {
                    }
                }
                closeJdbc(conn,statement,null);
            }
        }
        return returnTriggers;
    }

    /**
     * 创建双向同步的触发器
     * @param appName
     * @param tempTableName       临时表
     * @param tempStatusTableName 临时表_状态表
     * @param triggers             需要创建的触发器
     * @param tableAndFields
     * @param jdbc
     * @param isSourceToTarget
     * @return
     */
    public static Map<String,Map<String,String>> createTwoWayTriggers(String appName,String tempTableName,
                                                                      String tempStatusTableName,Map<String,Map<String,String>> triggers,
                                                   Map<TableBean,List<Field>> tableAndFields,Jdbc jdbc,boolean isSourceToTarget){
        String owner = jdbc.getDbOwner().toUpperCase();
        String sequenceName = "ICHANGE_SEQUENCE_" + appName.toUpperCase();
        Map<String,Map<String,String>> returnTriggers = new HashMap<String,Map<String,String>>();
        for (Map.Entry<TableBean,List<Field>> key: tableAndFields.entrySet()) {
            TableBean tableBean = key.getKey();
            String tableName = tableBean.getTableName();
            List<Field> pkList = key.getValue();
            Map<String,String> trigger = triggers.get(tableName);//触发器个数: I,I U,U D,D
            String newpatterns = "";   //for add,update trigger
            String oldpatterns = "";   //for delete trigger
            String pkNames = "";
            String pkTypes = "";
            int k = 0;
            for (Field field : pkList){
                if(k > 0){
                    newpatterns = newpatterns + "||','||";
                    oldpatterns = oldpatterns + "||','||";
                }
                k++;
                if("date".equalsIgnoreCase(field.getJdbcType()) || "date".equalsIgnoreCase(field.getDbType())){
                    newpatterns = newpatterns + "TO_CHAR(:new." + field.getFieldName() + ",'YYYY-MM-DD HH24:MI:SS')";
                    oldpatterns = oldpatterns + "TO_CHAR(:old." + field.getFieldName() + ",'YYYY-MM-DD HH24:MI:SS')";
                } else if("timestamp".equalsIgnoreCase(field.getJdbcType())){
                    newpatterns = newpatterns + "TO_CHAR(:new." + field.getFieldName() + ",'YYYY-MM-DD HH24:MI:SS.FF')";
                    oldpatterns = oldpatterns + "TO_CHAR(:old." + field.getFieldName() + ",'YYYY-MM-DD HH24:MI:SS.FF')";
                } else{
                    newpatterns = newpatterns + "TO_CHAR(:new." + field.getFieldName() + ")";
                    oldpatterns = oldpatterns + "TO_CHAR(:old." + field.getFieldName() + ")";
                }
                if(pkNames.length() == 0){
                    pkNames = isSourceToTarget?field.getFieldName():field.getDestField();
                }else{
                    pkNames = pkNames + "," + (isSourceToTarget?field.getFieldName():field.getDestField());
                }
                if(pkTypes.length() == 0){
                    pkTypes = field.getDbType();
                }else {
                    pkTypes = pkTypes + "," + field.getDbType();
                }
            }

            String select_I = newpatterns;//TO_CHAR(:new.ID)||','||TO_CHAR(:new.TEST1)
            String select_U = newpatterns;//TO_CHAR(:old.ID)||','||TO_CHAR(:old.TEST1)
            String select_D = oldpatterns;//TO_CHAR(:old.ID)||','||TO_CHAR(:old.TEST1)
            String pkStr = pkNames +";" + pkTypes;
            int hashCode = Math.abs((appName + tableName + tempTableName).hashCode());
            String triggerName = "ICHANGE_"+ hashCode +"";
            String insteadOf = "";
            if(tableBean.isView()){
                insteadOf = "instead of";
            } else {
                insteadOf = "after";
            }
            String Sql_TwoWay_Create_Trigger_Insert =
                "create or replace trigger "+triggerName+"_I "+insteadOf+" insert on "+owner+"."+tableName+" for each row " +
                    "DECLARE strVals varchar2(2550); action number;" +
                    "begin " +
                    "select action_status into action from "+owner+"."+tempStatusTableName+" where table_name = '"+tableName.toUpperCase()+"';" +
                    "if(action = 0) then " +
                    "select ("+select_I+") INTO strVals from dual; " +
                    "insert into "+owner+"."+tempTableName+" (id,dbname,tablename,pks,op) " +
                    "values ("+owner+"."+sequenceName+".nextval,'"+owner+"','"+tableName.toUpperCase()+"','"+pkStr+";'||strVals||';','I');" +
                    "else " +
                    "update "+owner+"."+tempStatusTableName+" set action_status = 0 where table_name='"+tableName.toUpperCase()+"';" +
                    "end if;"+
                "end;";
        String Sql_TwoWay_Create_Trigger_Update =
                "create or replace trigger "+triggerName+"_U "+insteadOf+" update on "+owner+"."+tableName+" for each row " +
                        "DECLARE strVals varchar2(2550); action number;" +
                        "begin " +
                        "select action_status into action from "+owner+"."+tempStatusTableName+" where table_name = '"+tableName.toUpperCase()+"';" +
                        "if(action = 0) then " +
                        "select ("+select_U+") INTO strVals from dual; " +
                        "insert into "+owner+"."+tempTableName+" (id,dbname,tablename,pks,op) " +
                        "values ("+owner+"."+sequenceName+".nextval,'"+owner+"','"+tableName.toUpperCase()+"','"+pkStr+";'||strVals||';','U');" +
                        "else " +
                        "update "+owner+"."+tempStatusTableName+" set action_status = 0 where table_name='"+tableName.toUpperCase()+"';" +
                        "end if;"+
                "end;";
        String Sql_TwoWay_Create_Trigger_Delete =
                "create or replace trigger "+triggerName+"_D "+insteadOf+" delete on "+owner+"."+tableName+" for each row " +
                        "DECLARE strVals varchar2(2550); action number;" +
                        "begin " +
                        "select action_status into action from "+owner+"."+tempStatusTableName+" where TABLE_NAME = '"+tableName.toUpperCase()+"';" +
                        "if(action = 0) then " +
                        "select ("+select_D+") INTO strVals from dual; " +
                        "insert into "+owner+"."+tempTableName+" (id,dbname,tablename,pks,op) " +
                        "values ("+owner+"."+sequenceName+".nextval,'"+owner+"','"+tableName.toUpperCase()+"','"+pkStr+";'||strVals||';','D');" +
                        "else " +
                        "update "+owner+"."+tempStatusTableName+" set action_status = 0 where table_name='"+tableName.toUpperCase()+"';" +
                        "end if;"+
                "end;";
            String Sql_Select_TempStatusTable = "select * from "+owner+"."+tempStatusTableName+" where TABLE_NAME = '"+tableName.toUpperCase()+"'";
            String Sql_Insert_TempStatusTable = "insert into "+owner+"."+tempStatusTableName+" (TABLE_NAME,ACTION_STATUS) values ('"+tableName.toUpperCase()+"',0)";
            Map<String,String> returnTrigger = new HashMap<String,String>();
            Connection conn = null;
            Statement statement = null;
            ResultSet rs = null;
            try {
                DataSource dataSource = DataSourceUtil.getDataSource(appName,jdbc);
                conn = dataSource.getConnection();
                statement = conn.createStatement();
                rs = statement.executeQuery(Sql_Select_TempStatusTable);
                boolean isNotExist = true;
                while (rs.next()){
                    String t = rs.getString("TABLENAME");
                    if(t.equals(tableName)){
                        isNotExist = false;
                    }
                }
                statement.close();
                conn.setAutoCommit(false);
                if(trigger.size()>0 && isNotExist){
                    statement = conn.createStatement();
                    statement.execute(Sql_Insert_TempStatusTable);
                }
                if(trigger.get("I")!=null){
                    statement = conn.createStatement();
                    statement.execute(Sql_TwoWay_Create_Trigger_Insert);
                    returnTrigger.put("I",triggerName+"_I");
                }
                if(trigger.get("U")!=null){
                    statement = conn.createStatement();
                    statement.execute(Sql_TwoWay_Create_Trigger_Update);
                    returnTrigger.put("U",triggerName+"_U");
                }
                if(trigger.get("D")!=null){
                    statement = conn.createStatement();
                    statement.execute(Sql_TwoWay_Create_Trigger_Delete);
                    returnTrigger.put("D",triggerName+"_D");
                }
                conn.commit();
                returnTriggers.put(tableName,returnTrigger);
            } catch (Exception e) {
                logger.error("应用"+appName+"创建"+(isSourceToTarget?"源":"目标")+"表"+tableName+"上的触发器错误"+e.getMessage(),e);
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                }
            } finally {
                if (conn != null) {
                    try {
                        conn.setAutoCommit(true);
                    } catch (SQLException e) {
                    }
                }
                closeJdbc(conn,statement,rs);
            }
        }
        return returnTriggers;
    }

    /**
     * 删除触发器
     * @param appName
     * @param triggers <tableName<I,triggerName U,triggerName D,triggerName>>
     * @param jdbc
     * @return
     */
    public static boolean deleteTrigger(String appName,Map<String,Map<String,String>> triggers,Jdbc jdbc) {
        String owner = jdbc.getDbOwner().toUpperCase();
        boolean isSuccess = false;
        Connection conn = null;
            PreparedStatement preStat = null;
            ResultSet rs = null;
            try {
                DataSource dataSource = DataSourceUtil.getDataSource(appName,jdbc);
                conn = dataSource.getConnection();
//                String Sql_Drop_Trigger = "drop trigger ?.?" ;
//                preStat = conn.prepareStatement(Sql_Drop_Trigger);
                for (Map.Entry<String,Map<String,String>> key: triggers.entrySet()) {
                    for (Map.Entry<String,String> kk : key.getValue().entrySet()){
//                        preStat.setString(1,owner);
//                        preStat.setString(2,kk.getValue());
                        String Sql_Drop_Trigger = "drop trigger "+owner+"."+kk.getValue() ;
                        preStat = conn.prepareStatement(Sql_Drop_Trigger);
                        preStat.execute();
                    }
                }
                conn.commit();
                isSuccess = true;
            } catch (Exception e) {
                logger.error("应用"+appName+"删除表上的触发器错误"+e.getMessage(),e);
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                }
            } finally {
                if (conn != null) {
                    try {
                        conn.setAutoCommit(true);
                    } catch (SQLException e) {
                    }
                }
                closeJdbc(conn,preStat,rs);
            }
        return isSuccess;
    }

    /**
     * 删除双向触发器
     * @param appName
     * @param triggers
     * @param jdbc
     * @param isSourceToTarget
     * @return
     */
    public static boolean deleteTwoWayTrigger(String appName,Map<String,Map<String,String>> triggers,Jdbc jdbc,boolean isSourceToTarget) {
        String owner = jdbc.getDbOwner().toUpperCase();
        boolean isSuccess = false;
        Connection conn = null;
            PreparedStatement preStat = null;
            ResultSet rs = null;
            try {
                DataSource dataSource = DataSourceUtil.getDataSource(appName,jdbc);
                conn = dataSource.getConnection();
                for (Map.Entry<String,Map<String,String>> key: triggers.entrySet()) {
                    for (Map.Entry<String,String> kk : key.getValue().entrySet()){
                        String Sql_Drop_Trigger = "drop trigger "+owner+"."+kk.getValue() ;
                        preStat = conn.prepareStatement(Sql_Drop_Trigger);
                        preStat.execute();
                    }
                }
                conn.commit();
                isSuccess = true;
            } catch (Exception e) {
                logger.error("应用"+appName+"删除"+(isSourceToTarget?"源":"目标")+"上的触发器错误"+e.getMessage(),e);
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                }
            } finally {
                if (conn != null) {
                    try {
                        conn.setAutoCommit(true);
                    } catch (SQLException e) {
                    }
                }
                closeJdbc(conn,preStat,rs);
            }
        return isSuccess;
    }

    private static void closeJdbc(Connection conn, Statement statement, ResultSet rs) {
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
