package com.hzih.db;

import com.hzih.db.entity.Type;
import com.inetec.common.config.nodes.Jdbc;

/**
 * Created by Administrator on 15-10-29.
 */
public interface Operator extends Runnable{

    public void init(Type type);

    public boolean isRun();

    public void stopThread();

    public void config(Jdbc sourceJdbc, Jdbc targetJdbc);
}
