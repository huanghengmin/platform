package com.hzih.utils;

import com.hzih.db.entity.Type;
import com.hzih.db.utils.Configuration;
import junit.framework.TestCase;


/**
 * Created by Administrator on 15-11-9.
 */
public class TestConfiguration extends TestCase{

    public static final String configXml = System.getProperty("ichange.home")+"/repository/config_db.xml";


    public void testType(){
        System.setProperty("ichange.home","D:/app/ichange");
        String configXml = System.getProperty("ichange.home")+"/repository/config_db.xml";
//        String appName = "dbsx1";
//        String appName = "dbcf1";
//        String appName = "dbqb";
//        String appName = "dbbj";
//        String appName = "dbsjbj";
        String appName = "dbxl";
        try {
            Type type = Configuration.getType(appName,configXml);
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}
