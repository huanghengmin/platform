package com.hzih.db.utils;

/**
 * Created by Administrator on 15-11-9.
 */
public class MathUtils {
    public static int[] getIntegerArray(String elemInfo) {
        if("null".equals(elemInfo)||"\"null\"".equalsIgnoreCase(elemInfo)) {
            return null;
        }
        String[] elems = elemInfo.split("\\|");
        int[] ele = new int[elems.length];
        for (int i = 0; i < elems.length; i ++) {
            String ee = elems[i];
            ele[i] = Integer.parseInt(elems[i]);
        }
        return ele;
    }

    public static double[] getDoubleArray(String ordinate) {

        if("null".equals(ordinate)||"\"null\"".equalsIgnoreCase(ordinate)) {
            return null;
        }
        String[] ordi = ordinate.split("\\|");
        double[] o = new double[ordi.length];
        for (int i = 0; i < ordi.length; i ++) {
            o[i] = Double.parseDouble(ordi[i]);
        }
        return o;
    }
}
