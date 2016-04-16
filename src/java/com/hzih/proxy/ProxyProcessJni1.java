package com.hzih.proxy;

import java.io.File;
import java.lang.reflect.Method;

public class ProxyProcessJni1 {

    static {
        Method llm;
        try{
            llm = ClassLoader.class.getDeclaredMethod("loadLibrary0", Class.class, File.class);
            llm.setAccessible(true);
            llm.invoke(null,new Object[]{ProxyProcessJni1.class,new File(System.getProperty("ichange.home")+"/others/libPortProxyMap.so")});
        } catch (Exception e) {
        }
    }

    private native int add_tunnel(
                                  String listenHost,int listenPort,
                                  String dstHost,int dstPort,
                                  int timeout,int proxyType);

    private native int del_tunnel(int listenPort,int proxyType);

    private native double[] get_traffic(int listenPort,int proxyType);
}
