package com.hzih.db.entity;

import com.inetec.common.config.nodes.Field;

/**
 * Created by Administrator on 15-10-30.
 */
public class FieldValue extends Field{

    private Object obj;

    public Object getObj() {
        return obj;
    }

    public void setObj(Object obj) {
        this.obj = obj;
    }
}
