package com.hzih.db.twoway;

import com.hzih.db.Operator;
import com.hzih.db.entity.Table;
import com.hzih.db.entity.Type;
import com.inetec.common.config.nodes.Jdbc;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Created by Administrator on 15-10-29.
 */
public class OperatorTwoWay implements Operator {

    private static final Logger logger = Logger.getLogger(OperatorTwoWay.class);
    private boolean isRun = false;
    private boolean isStop = false;
    private Type type;
    private String appName;
    private boolean isNormal = false;//普通关系,非依赖

    private Jdbc sourceJdbc;
    private Jdbc targetJdbc;

    @Override
    public void init(Type type) {
        this.type = type;
        this.appName = type.getAppName();

        List<Table> list = type.getTableList();
        Map<Integer,List<Table>> tableListMap = new HashMap<Integer, List<Table>>();
        List<Integer> numberList = new ArrayList<>();
        for (Table table : list){
            int seqNumber = table.getTableSeqNumber();
            if(tableListMap.get(seqNumber)==null){
                List<Table> tables = new ArrayList<Table>();
                tables.add(table);
                tableListMap.put(seqNumber, tables);
                numberList.add(seqNumber);
            } else {
                List<Table> tables = tableListMap.get(seqNumber);
                tables.add(table);
                tableListMap.put(seqNumber,tables);
            }
        }
        if(tableListMap.size()>1) {
            isNormal = false;
            Integer[] seqArray = (Integer[])numberList.toArray(new Integer[numberList.size()]);
            Arrays.sort(seqArray);
            List<Table> newList = new ArrayList<Table>();
            for (int i = 0; i< seqArray.length;i++) {
                List<Table> tables = tableListMap.get(seqArray[i]);
                for (Table table : tables){
                    newList.add(table);
                }
            }
            type.setTableList(newList);
        } else {
            isNormal = true;
        }
    }

    @Override
    public void config(Jdbc sourceJdbc, Jdbc targetJdbc){
        this.sourceJdbc = sourceJdbc;
        this.targetJdbc = targetJdbc;
    }

    @Override
    public boolean isRun() {
        return isRun;
    }

    @Override
    public void stopThread() {
        isStop = true;
    }

    @Override
    public void run() {
        isRun = true;
        try{
            if (isNormal) {
                List<Table> list = type.getTableList();
                for (Table table : list){
                    String sourceTableName = table.getSourceTableName();
                    String targetTableName = table.getTargetTableName();
                    boolean isSourceToTarget = true;
                    TowWayNormal normalSource = new TowWayNormal();//源到目标
                    normalSource.config(sourceJdbc, targetJdbc,isSourceToTarget);
                    normalSource.init(type,sourceTableName,targetTableName,table,isSourceToTarget);
                    normalSource.start();
                    isSourceToTarget = false;
                    TowWayNormal normalTarget = new TowWayNormal();//目标到源
                    normalTarget.config(sourceJdbc, targetJdbc,isSourceToTarget);
                    normalTarget.init(type,sourceTableName,targetTableName,table,isSourceToTarget);
                    normalTarget.start();

                }
            } else {
                boolean isSourceToTarget = true;
                TwoWayRely relySource = new TwoWayRely();
                relySource.config(sourceJdbc, targetJdbc, isSourceToTarget);
                relySource.init(type, isSourceToTarget);
                relySource.start();
                isSourceToTarget = false;
                TwoWayRely relyTarget = new TwoWayRely();
                relyTarget.config(sourceJdbc, targetJdbc, isSourceToTarget);
                relyTarget.init(type, isSourceToTarget);
                relyTarget.start();
            }
            logger.info(appName + "应用启动");
        } catch (Exception e){
            logger.error(appName + "启动错误" + e.getMessage(),e);
        }
        while (isRun) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                logger.error(appName + "应用启动等待错误" +e.getMessage(), e);
            }
        }

    }
}
