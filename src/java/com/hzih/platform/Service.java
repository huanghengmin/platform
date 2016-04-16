package com.hzih.platform;

import com.hzih.db.entity.Type;
import com.inetec.common.config.nodes.Jdbc;

/**
 * Created by Administrator on 15-10-29.
 */
public interface Service extends Runnable {

    public void init(Type type, Jdbc sourceJdbc,Jdbc targetJdbc);

    public boolean isOperatorStopped();

    public void stopThread();

    public boolean isRun();
}
