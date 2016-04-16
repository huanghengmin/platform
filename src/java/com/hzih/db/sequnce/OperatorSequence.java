package com.hzih.db.sequnce;

import com.hzih.db.Operator;
import com.hzih.db.entity.Sequence;
import com.hzih.db.entity.Table;
import com.hzih.db.entity.Type;
import com.inetec.common.config.nodes.Jdbc;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by Administrator on 15-10-29.
 */
public class OperatorSequence implements Operator {

    private static final Logger logger = Logger.getLogger(OperatorSequence.class);
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
        isNormal = true;

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
        if (isNormal) {
            List<Sequence> sequenceList = type.getSequenceList();
            for (Sequence sequence : sequenceList) {
                String sourceSequenceName = sequence.getSourceSequenceName();
                String targetSequenceName = sequence.getTargetSequenceName();
                SequenceNormal normal = new SequenceNormal();
                normal.init(type,sourceSequenceName,targetSequenceName,null,sourceJdbc, targetJdbc);
                normal.start();
            }
        }
        logger.info(appName + "应用启动");
        while (isRun) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                logger.error(appName + "应用启动等待错误" + e.getMessage(), e);
            }
        }

    }
}
