package com.hzih.platform;

import com.hzih.db.dateMarker.OperatorDateMarker;
import com.hzih.db.entirely.OperatorEntirely;
import com.hzih.db.marker.OperatorMarker;
import com.hzih.utils.PlatformUtils;
import com.hzih.db.Operator;
import com.hzih.db.entity.Type;
import com.hzih.db.sequnce.OperatorSequence;
import com.hzih.db.trigger.OperatorTrigger;
import com.hzih.db.twoway.OperatorTwoWay;
import com.hzih.jdbc.DataSourceUtil;
import com.inetec.common.config.nodes.Jdbc;
import org.apache.log4j.Logger;


/**
 * Created by Administrator on 15-10-28.
 */
public class DBService implements Service{

    private final static Logger logger = Logger.getLogger(DBService.class);

    private String appName;
    private Operator operator;
    private boolean isRun = false;

    @Override
    public void init(Type type, Jdbc sourceJdbc,Jdbc targetJdbc){
        this.appName = type.getAppName();
        try{
            DataSourceUtil.source = PlatformUtils.jdbcToMap(sourceJdbc);
            DataSourceUtil.target = PlatformUtils.jdbcToMap(targetJdbc);

            String appType = type.getAppType();

            if("db_sx".equalsIgnoreCase(appType)){//双向
                operator = new OperatorTwoWay();
            }else if("db_cf".equalsIgnoreCase(appType)){//触发
                operator = new OperatorTrigger();
            }else if("db_qb".equalsIgnoreCase(appType)){//全表
                operator = new OperatorEntirely();
            }else if("db_bj".equalsIgnoreCase(appType)){//标记
                operator = new OperatorMarker();
            }else if("db_sjbj".equalsIgnoreCase(appType)){//时间标记
                operator = new OperatorDateMarker();
            }else if("db_xl".equalsIgnoreCase(appType)){//sequence
                operator = new OperatorSequence();
            } else {
                logger.warn(appName + " 对应的模式["+appType+"]不存在...");
            }
            if(operator!=null){
                operator.config(sourceJdbc,targetJdbc);
                operator.init(type);
                new Thread(operator).start();
            }
        }  catch (Exception e){
            logger.error(appName + " DBService加载错误" + e.getMessage(),e);
        }
    }

    @Override
    public void stopThread(){
        operator.stopThread();
    }

    @Override
    public boolean isOperatorStopped() {
        return operator.isRun();
    }

    @Override
    public boolean isRun() {
        return isRun;
    }



    @Override
    public void run() {
        isRun = true;
        while (isRun) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }

        }
    }
}
