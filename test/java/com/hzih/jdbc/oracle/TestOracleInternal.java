package com.hzih.jdbc.oracle;

import com.hzih.console.ConsoleServlet;
import com.inetec.common.config.ConfigParser;
import com.inetec.common.config.nodes.Field;
import com.inetec.common.config.nodes.IChange;
import com.inetec.common.config.nodes.Jdbc;
import com.inetec.common.config.nodes.Table;
import com.inetec.common.exception.Ex;
import junit.framework.TestCase;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 15-11-5.
 */
public class TestOracleInternal extends TestCase{

    private final static Logger logger = Logger.getLogger(TestOracleInternal.class);


    public void testOracleCreateSingle(){
        System.setProperty("ichange.home","D:/app/ichange");
        ConfigParser configParser = null;
        try {
            configParser = new ConfigParser(ConsoleServlet.configXml);
            IChange iChange = configParser.getRoot();
            Jdbc jdbc = iChange.getJdbc("orcl251");

            String appName = "test_single";
            String tempTableName = "ICHANGE_" + appName;
            boolean isSuccess = OracleInternal.createSingleTempTable(appName,tempTableName,jdbc);
            logger.info("创建临时表"+tempTableName+(isSuccess?"成功":"失败"));



            Map<String,Map<String,String>> triggers = new HashMap<String,Map<String,String>>();
            Map<String,String> trigger = new HashMap<String,String>();
            String tableName = "test_db_4";
            trigger.put("I","I");
            trigger.put("U","U");
            trigger.put("D","D");
            triggers.put(tableName,trigger);
            Map<TableBean,List<Field>> tableAndFields = new HashMap<TableBean,List<Field>>();
            List<Field> list = new ArrayList<Field>();
            Field field = new Field();
            field.setFieldName("ID");
            field.setJdbcType("NUMERIC");
            field.setDbType("NUMBER");
            field.setPk("true");
            field.setDestField("ID");
            list.add(field);
            field = new Field();
            field.setFieldName("TEST1");
            field.setJdbcType("VARCHAR");
            field.setDbType("VARCHAR2");
            field.setPk("true");
            field.setDestField("ID");
            list.add(field);
            TableBean tableBean = new TableBean(tableName,false);
            tableAndFields.put(tableBean,list);
            Map<String,Map<String,String>> triggerNames = OracleInternal.createSingleTriggers(appName,tempTableName,triggers,tableAndFields,jdbc);
            for (Map.Entry<String,Map<String,String>> key: triggerNames.entrySet()) {
                for (Map.Entry<String,String> t:key.getValue().entrySet()){
                    logger.info("表" +  key.getKey() + "创建触发器"+t.getValue());
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testOracleDropSingle(){
        System.setProperty("ichange.home","D:/app/ichange");
        ConfigParser configParser = null;
        try {
            configParser = new ConfigParser(ConsoleServlet.configXml);
            IChange iChange = configParser.getRoot();
            Jdbc jdbc = iChange.getJdbc("orcl251");

            String appName = "test_single";
            String tempTableName = "ICHANGE_" + appName;
            boolean isSuccess = OracleInternal.deleteSingleTempTable(appName,tempTableName,jdbc);
            logger.info("删除临时表"+tempTableName+(isSuccess?"成功":"失败"));

            Map<String,Map<String,String>> triggers = new HashMap<String,Map<String,String>>();
            Map<String,String> trigger = new HashMap<String,String>();
            String tableName = "test_db_4";
            int hashCode = Math.abs((appName + tableName + tempTableName).hashCode());
            String triggerName = "ICHANGE_"+ hashCode +"";
            trigger.put("I", triggerName+"_I");
            trigger.put("U", triggerName+"_U");
            trigger.put("D", triggerName+"_D");
            triggers.put(tableName, trigger);
            isSuccess = OracleInternal.deleteTrigger(appName,triggers,jdbc);
            logger.info("删除表" + tableName + "的触发器" + (isSuccess ? "成功" : "失败"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testOracleCreateTwoWay(){
        System.setProperty("ichange.home","D:/app/ichange");
        ConfigParser configParser = null;
        try {
            configParser = new ConfigParser(ConsoleServlet.configXml);
            IChange iChange = configParser.getRoot();
            Jdbc jdbc = iChange.getJdbc("orcl251");

            String appName = "test_two";
            String tempTableName = "ICHANGE_" + appName.toUpperCase();
            String tempStatusTableName = "ICHANGE_S_" + appName.toUpperCase();
            boolean isSuccess = OracleInternal.createTwoWayTempTable(appName, tempTableName, tempStatusTableName, jdbc);
            logger.info("创建临时表"+tempTableName+"和"+tempStatusTableName+(isSuccess?"成功":"失败"));

            Map<String,Map<String,String>> triggers = new HashMap<String,Map<String,String>>();
            Map<String,String> trigger = new HashMap<String,String>();
            String tableName = "test_db_4".toUpperCase();
            trigger.put("I","I");
            trigger.put("U","U");
            trigger.put("D","D");
            triggers.put(tableName,trigger);
            Map<TableBean,List<Field>> tableAndFields = new HashMap<TableBean,List<Field>>();
            List<Field> list = new ArrayList<Field>();
            Field field = new Field();
            field.setFieldName("ID");
            field.setJdbcType("NUMERIC");
            field.setDbType("NUMBER");
            field.setPk("true");
            field.setDestField("ID");
            list.add(field);
            field = new Field();
            field.setFieldName("TEST1");
            field.setJdbcType("VARCHAR");
            field.setDbType("VARCHAR2");
            field.setPk("true");
            field.setDestField("ID");
            list.add(field);
            TableBean tableBean = new TableBean(tableName,false);
            tableAndFields.put(tableBean,list);
            boolean isSourceToTarget = true;
            Map<String,Map<String,String>> triggerNames = OracleInternal.createTwoWayTriggers(appName, tempTableName,
                    tempStatusTableName, triggers, tableAndFields, jdbc, isSourceToTarget);
            for (Map.Entry<String,Map<String,String>> key: triggerNames.entrySet()) {
                for (Map.Entry<String,String> t:key.getValue().entrySet()){
                    logger.info((isSourceToTarget?"源":"目标")+"表" +  key.getKey() + "创建触发器"+t.getValue());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testOracleDropTwoWay(){
        System.setProperty("ichange.home","D:/app/ichange");
        ConfigParser configParser = null;
        try {
            configParser = new ConfigParser(ConsoleServlet.configXml);
            IChange iChange = configParser.getRoot();
            Jdbc jdbc = iChange.getJdbc("orcl251");

            String appName = "test_two";
            String tempTableName = "ICHANGE_" + appName.toUpperCase();
            String tempStatusTableName = "ICHANGE_S_" + appName.toUpperCase();
            boolean isSuccess = OracleInternal.deleteTwoWayTempTable(appName, tempTableName, tempStatusTableName, jdbc);
            logger.info("删除临时表"+tempTableName+"和"+tempStatusTableName+(isSuccess?"成功":"失败"));

            Map<String,Map<String,String>> triggers = new HashMap<String,Map<String,String>>();
            Map<String,String> trigger = new HashMap<String,String>();
            String tableName = "test_db_4".toUpperCase();
            int hashCode = Math.abs((appName + tableName + tempTableName).hashCode());
            String triggerName = "ICHANGE_"+ hashCode +"";
            trigger.put("I", triggerName+"_I");
            trigger.put("U", triggerName+"_U");
            trigger.put("D", triggerName+"_D");
            triggers.put(tableName, trigger);
            isSuccess = OracleInternal.deleteTrigger(appName,triggers,jdbc);
            logger.info("删除表" + tableName + "的触发器" + (isSuccess ? "成功" : "失败"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
