package com.hzih.proxy;

import com.hzih.platform.ProxyService;
import junit.framework.TestCase;

/**
 * Created by Administrator on 15-11-5.
 */
public class TestProxy extends TestCase{


    public void testStartLink(){
        String param = "192.168.1.1|8080:8090|172.16.1.1|8080:8090|60|2";
        String paramStop = "8080:8090|2";
        try {
            ProxyService p = new ProxyService();
            p.init("proxyApp");
            p.startLink(param);
            p.stopLink(paramStop);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
