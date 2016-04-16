package com.hzih.db.utils;

import oracle.spatial.geometry.JGeometry;
import oracle.sql.STRUCT;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 14-12-1
 * Time: 下午1:38
 * To change this template use File | Settings | File Templates.
 */
public class MyJGeometry extends JGeometry {


    public MyJGeometry(int i, int i1, double v, double v1, double v2, int[] ints, double[] doubles) {
        super(i, i1, v, v1, v2, ints, doubles);
    }

    public synchronized static void clearDBDescriptors() {
        geomDesc = null;
        pointDesc = null;
        elemInfoDesc = null;
        ordinatesDesc = null;
    }

    public synchronized static STRUCT restore(int i, int i1, double v, double v1, double v2, int[] ints, double[] doubles,Connection conn) throws SQLException {
        clearDBDescriptors();
        MyJGeometry myJGeometry = new MyJGeometry(i, i1, v, v1, v2, ints, doubles);
        STRUCT struct = store(myJGeometry,conn);
        return struct;
    }





}
