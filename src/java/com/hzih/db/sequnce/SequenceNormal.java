package com.hzih.db.sequnce;

import com.hzih.db.entity.SequenceValue;
import com.hzih.db.entity.Table;
import com.hzih.db.entity.Type;
import com.hzih.jdbc.DataSourceUtil;
import com.inetec.common.config.nodes.Jdbc;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.*;


/**
 * Created by Administrator on 15-10-29.
 */
public class SequenceNormal extends Thread {

    private static final Logger logger = Logger.getLogger(SequenceNormal.class);

    private boolean isRun = false;
    private boolean isStop = false;
    private DataSource dataSource = null;
    private DataSource dataTarget = null;
    private String appName;

    private String sourceSequenceName;
    private String targetSequenceName;

    private String Sql_SelectFromSource;
    private String Sql_SelectFromTarget;
    private String Sql_SelectFromSourceSequences;
    private String Sql_SelectFromTargetSequences;
    private int interval;

    public void init(Type type, String sourceSequenceName, String targetSequenceName,
                     Table table, Jdbc sourceJdbc, Jdbc targetJdbc) {
        this.appName = type.getAppName();
        this.sourceSequenceName = sourceSequenceName;
        this.targetSequenceName = targetSequenceName;
        try {
            dataSource = DataSourceUtil.getDataSource(DataSourceUtil.DRUID_SOURCE);
            dataTarget = DataSourceUtil.getDataSource(DataSourceUtil.DRUID_TARGET);
        } catch (Exception e) {
            logger.error(appName + " 建立数据源失败" + e.getMessage(),e);
        }

        interval = type.getInterval();

        /** 查询源表语句 还需根据实际情况加上查询条件 **/
        Sql_SelectFromSource = "select " + sourceSequenceName+".CURRVAL,"+sourceSequenceName+".NEXTVAL from dual";
        Sql_SelectFromTarget = "select " + targetSequenceName+".CURRVAL,"+targetSequenceName+".NEXTVAL from dual";

        Sql_SelectFromSourceSequences = "select * from "+ SequenceValue.ALL_SEQUENCES
                +" where "+SequenceValue.SEQUENCE_OWNER +" = '"+sourceJdbc.getDbOwner().toUpperCase()
                +"' and "+ SequenceValue.SEQUENCE_NAME + " = '"+sourceSequenceName.toUpperCase()+"'";
        Sql_SelectFromTargetSequences = "select * from "+ SequenceValue.ALL_SEQUENCES
                +" where "+SequenceValue.SEQUENCE_OWNER +" = '"+targetJdbc.getDbOwner().toUpperCase()
                +"' and "+ SequenceValue.SEQUENCE_NAME +" = '"+targetSequenceName.toUpperCase()+"'";
        logger.info(appName + "应用同步,序列"
                + sourceSequenceName + "同步到序列" + targetSequenceName + "配置加载成功.");
    }



    public boolean isRun() {
        return isRun;
    }

    public void stopThread() {
        isStop = true;
    }

    /**
     //取临时表数据:单独一个表
     //取真实数据
     //转换成IUD 三种sql语句
     //提交给目标端
     //删除临时表
     */
    @Override
    public void run() {
        isRun = true;
        SequenceValue sequenceSource = selectFromSequences(dataTarget, Sql_SelectFromSourceSequences, sourceSequenceName, false);
        SequenceValue sequenceTarget = selectFromSequences(dataTarget, Sql_SelectFromTargetSequences, targetSequenceName, false);
        boolean isLastNumberMode = false;
        if(sequenceSource.getIncrement() == sequenceTarget.getIncrement()){
            if(sequenceSource.getCacheSize() == sequenceTarget.getCacheSize()
                    && sequenceSource.getCacheSize() == sequenceSource.getIncrement()
                     ) {
                isLastNumberMode = true;
                logger.info(appName + "应用源序列和目标序列cache_size和increment_by相同,采用last_number值作为序列同步值");
            }
        }
        long sourceIncrementBy = sequenceSource.getIncrement();
        long targetIncrementBy = sequenceTarget.getIncrement();
        boolean isFirst = true;
        while (isRun) {
            if(!isStop) {
                do{
                    try{
                        long sourceLastValue;
                        long targetLastValue;
                        if(isLastNumberMode){
                            if(!isFirst){
                                sequenceSource = selectFromSequences(dataTarget, Sql_SelectFromSourceSequences, sourceSequenceName, false);
                                sequenceTarget = selectFromSequences(dataTarget, Sql_SelectFromTargetSequences, targetSequenceName, false);
                            } else {
                                isFirst = false;
                            }
                            sourceLastValue = sequenceSource.getLastNumber();
                            targetLastValue = sequenceTarget.getLastNumber();
                        } else {
                            sourceLastValue = selectFromSequence(dataSource,Sql_SelectFromSource,sourceSequenceName,true);
                            targetLastValue = selectFromSequence(dataTarget,Sql_SelectFromTarget,targetSequenceName,true);
                        }
                        if(targetLastValue == sourceLastValue) {
                            try {
                                Thread.sleep(1000 * interval);
                            } catch (InterruptedException e) {
                            }
                            continue;
                        }
                        boolean isSuccess = updateToTarget(sourceLastValue,targetLastValue,sourceIncrementBy,targetIncrementBy);
                    } catch (Exception e){
                        logger.error(appName + "应用同步,错误"+e.getMessage(),e);
                    }
                } while (!isStop);
            } else {
                isRun = false;
            }
        }
    }

    /**
     * 获取序列的当前值记录
     * @return
     */
    private long selectFromSequence(DataSource dataSource,String Sql_SelectFromSource,String sequenceName,boolean isSource) {
        Connection conn = null;
        Statement statement = null;
        ResultSet rs = null;
        long currval = 0;
        try{
            conn = dataSource.getConnection();
            statement = conn.createStatement();
            rs = statement.executeQuery(Sql_SelectFromSource);
            while (rs.next()){
                currval = rs.getLong(SequenceValue.CURRVAL);
            }
        } catch (Exception e) {
            logger.error("应用" + appName + "读取"+(isSource?"源":"目标")+"序列"+sequenceName+ "的记录失败,原因:" + e.getMessage(),e);
            try {
                Thread.sleep(1000 * 60);
            } catch (InterruptedException e1) {
            }
        } finally {
            closeJdbc(conn,statement,rs);
        }
        return currval;
    }

    /**
     * 获取序列的属性
     * @param dataSource
     * @param sql
     * @param sequenceName
     * @param isSource
     * @return
     */
    private SequenceValue selectFromSequences(DataSource dataSource,String sql,String sequenceName,boolean isSource) {
        Connection conn = null;
        Statement statement = null;
        ResultSet rs = null;
        long incrementBy = 0;
        long lastNumber = 0;
        long cacheSize = 0;
        try{
            conn = dataSource.getConnection();
            statement = conn.createStatement();
            rs = statement.executeQuery(sql);
            while (rs.next()){
                incrementBy = rs.getLong(SequenceValue.INCREMENT_BY);
                lastNumber = rs.getLong(SequenceValue.LAST_NUMBER);
                cacheSize = rs.getLong(SequenceValue.CACHE_SIZE);
            }
        } catch (Exception e) {
            logger.error("应用" + appName + "读取"+(isSource?"源":"目标")+"序列"+sequenceName+ "的自增长值失败,原因:" + e.getMessage(),e);
        } finally {
            closeJdbc(conn,statement,rs);
        }
        SequenceValue sequence = new SequenceValue();
        sequence.setIncrement(incrementBy);
        sequence.setCacheSize(cacheSize);
        sequence.setLastNumber(lastNumber);
        return sequence;
    }


    /**
     * 更新序列
     * @param sourceSequence
     * @param targetSequence
     * @param sourceIncrementBy
     * @param targetIncrementBy
     * @return
     */
    private boolean updateToTarget(long sourceSequence, long targetSequence, long sourceIncrementBy,long targetIncrementBy) {
        boolean isSuccess = false;
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        boolean isSource = sourceSequence < targetSequence;
        try{
            if(isSource){
                conn = dataSource.getConnection();
            } else {
                conn = dataTarget.getConnection();
            }
            conn.setAutoCommit(false);
            if(sourceSequence > targetSequence) {
                long newIncrement = sourceSequence - targetSequence;//目标和源相差值
                if(newIncrement != targetIncrementBy){
                    String Sql_UpdateToTarget_1 = "alter sequence "+targetSequenceName+" increment by " + newIncrement;
                    String Sql_UpdateToTarget_2 = "select "+targetSequenceName+".nextval from dual";
                    String Sql_UpdateToTarget_3 = "alter sequence "+targetSequenceName+" increment by " + targetIncrementBy;
                    preparedStatement = conn.prepareStatement(Sql_UpdateToTarget_1);
                    preparedStatement.execute();
                    preparedStatement = conn.prepareStatement(Sql_UpdateToTarget_2);
                    preparedStatement.execute();
                    preparedStatement = conn.prepareStatement(Sql_UpdateToTarget_3);
                    preparedStatement.execute();
                } else {
                    String Sql_UpdateToTarget_2 = "select "+targetSequenceName+".nextval from dual";
                    preparedStatement = conn.prepareStatement(Sql_UpdateToTarget_2);
                    preparedStatement.execute();
                }
            } else {
                long newIncrement = targetSequence - sourceSequence;//目标和源相差值
                if(newIncrement != sourceIncrementBy){
                    String Sql_UpdateToSource_1 = "alter sequence "+sourceSequenceName+" increment by " + newIncrement;
                    String Sql_UpdateToSource_2 = "select "+sourceSequenceName+".nextval from dual";
                    String Sql_UpdateToSource_3 = "alter sequence "+sourceSequenceName+" increment by " + sourceIncrementBy;
                    preparedStatement = conn.prepareStatement(Sql_UpdateToSource_1);
                    preparedStatement.execute();
                    preparedStatement = conn.prepareStatement(Sql_UpdateToSource_2);
                    preparedStatement.execute();
                    preparedStatement = conn.prepareStatement(Sql_UpdateToSource_3);
                    preparedStatement.execute();
                } else {
                    String Sql_UpdateToSource_2 = "select "+sourceSequenceName+".nextval from dual";
                    preparedStatement = conn.prepareStatement(Sql_UpdateToSource_2);
                    preparedStatement.execute();
                }
            }
            logger.error("应用" + appName + "更新"+(isSource ? "源":"目标")+"序列"
                    +(isSource?sourceSequenceName:targetSequenceName)+ "的记录");
            isSuccess = true;
        } catch (Exception e) {
            logger.error("应用" + appName + "更新"+(isSource ? "源":"目标")+"序列"
                    +(isSource?sourceSequenceName:targetSequenceName)+ "的记录失败,原因:" + e.getMessage(),e);
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                }
            }
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                }
            }
            closeJdbc(conn, preparedStatement, rs);
        }
        return isSuccess;
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
