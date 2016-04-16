package com.hzih.platform;

import com.hzih.proxy.ProxyProcessJni;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 15-11-3.
 */
public class ProxyService extends Thread {
    private final static Logger logger = Logger.getLogger(ProxyService.class);
    private boolean isRun = false;
    private ProxyProcessJni proxy = new ProxyProcessJni();
    private Map<String,String> portMap = new HashMap<String,String>();

    private String appName;

    public void init(String appName) {
        this.appName = appName;
        File file = new File(System.getProperty("ichange.home")+"/others/libPortProxyMap.so");
        if(!file.exists()){
            logger.error("代理组件的../others/libPortProxyMap.so文件缺失");
        }
    }


    public void addLink(String listenHost,int listenPort,
                        String dstHost,int dstPort,
                        int timeout,int proxyType) {
        logger.info(appName + "应用启动" + (proxyType == 0 ? "TCP" : "UDP") + "代理服务,监听:" + listenHost + ":" + listenPort + "目标:" + dstHost + ":" + dstPort);
        this.proxy.addLink(appName,listenHost, listenPort, dstHost, dstPort, timeout, proxyType);
        portMap.put( proxyType + "-" + listenPort,proxyType + "-" + listenPort);
    }

    public void deleteLink(int listenPort,int proxyType){
        logger.info(appName + "应用停止" + (proxyType == 0 ? "TCP" : "UDP") + "代理服务,监听于:" + listenPort);
        this.proxy.deleteLink(appName,listenPort,proxyType);
        portMap.remove( proxyType + "-" + listenPort);
    }


    public boolean isOperatorStopped() {
        return false;
    }


    public void stopThread() {

    }


    public boolean isRun() {
        return isRun;
    }

    @Override
    public void run() {
        isRun = true;
        int listenPort = 0;
        int proxyType = 0;
        String[] keys;
        String key;
        while (isRun) {
            try{
                for (Map.Entry<String,String> entry:portMap.entrySet()){
                    key = entry.getKey();
                    keys = key.split("-");
                    listenPort = Integer.parseInt(keys[0]);
                    proxyType = Integer.parseInt(keys[1]);
                    double[] flux = this.proxy.getFlux(listenPort,proxyType);

                }
            } catch (Exception e){
                logger.error("通用代理取流量错误",e);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
    }

    public void startLink(String param) throws Exception{
        String[] params = param.split("\\|");
        int index = 0;
        String listenHost = params[index++];
        String listenPortStr = params[index++];
        String dstHost = params[index++];
        String dstPortStr = params[index++];
        int timeout = Integer.parseInt(params[index++]);
        int proxyType = Integer.parseInt(params[index++]);//0:tcp,1:udp,2:tcp+udp
        int listenPort;
        int listenPortMax;
        if(listenPortStr.indexOf("-")>-1){
            listenPort = Integer.parseInt(listenPortStr.split("-")[0]);
            listenPortMax = Integer.parseInt(listenPortStr.split("-")[1]);
        } else if(listenPortStr.indexOf(":")>-1){
            listenPort = Integer.parseInt(listenPortStr.split(":")[0]);
            listenPortMax = Integer.parseInt(listenPortStr.split(":")[1]);
        } else {
            listenPort = Integer.parseInt(listenPortStr);
            listenPortMax = 0;
        }
        int dstPort;
        int dstPortMax;
        if(dstPortStr.indexOf("-")>-1){
            dstPort = Integer.parseInt(dstPortStr.split("-")[0]);
            dstPortMax = Integer.parseInt(dstPortStr.split("-")[1]);
        } else if(dstPortStr.indexOf(":")>-1){
            dstPort = Integer.parseInt(dstPortStr.split(":")[0]);
            dstPortMax = Integer.parseInt(dstPortStr.split(":")[1]);
        } else {
            dstPort = Integer.parseInt(dstPortStr);
            dstPortMax = 0;
        }
        int[] dstPorts = new int[0];
        if(dstPortMax > 0){
            dstPorts = new int[dstPortMax-dstPort+1];
            int idx = 0;
            for (int i = dstPort; i<= dstPortMax; i++){
                dstPorts[idx++] = i;
            }
        }
        if(proxyType>1){
            if(listenPortMax > 0){
                int idx = 0;
                for (int i = listenPort; i<= listenPortMax; i++){
                    int dstP = dstPorts[idx++];
                    addLink(listenHost, i,
                            dstHost, dstP, timeout, 0);
                    addLink(listenHost, i,
                            dstHost, dstP, timeout, 1);
                }
            } else {
                addLink(listenHost, listenPort,
                        dstHost, dstPort, timeout, 0);
                addLink(listenHost, listenPort,
                        dstHost, dstPort, timeout, 1);
            }
        } else {
            if(listenPortMax > 0){
                int idx = 0;
                for (int i = listenPort; i<= listenPortMax; i++){
                    int dstP = dstPorts[idx++];
                    addLink(listenHost, i,
                            dstHost, dstP, timeout, proxyType);
                }
            } else {
                addLink(listenHost, listenPort,
                        dstHost, dstPort, timeout, proxyType);
            }
        }
    }

    public void stopLink(String param) throws Exception{
        String[] params = param.split("\\|");
        int index = 0;
        String listenHost = params[index++];
        String listenPortStr = params[index++];
        String dstHost = params[index++];
        String dstPortStr = params[index++];
        int timeout = Integer.parseInt(params[index++]);
        String proxyTypeStr = params[index++];
        int proxyType = Integer.parseInt(proxyTypeStr);//0:tcp,1:udp,2:tcp+udp
        int listenPort;
        int listenPortMax;
        if(listenPortStr.indexOf("-")>-1){
            listenPort = Integer.parseInt(listenPortStr.split("-")[0]);
            listenPortMax = Integer.parseInt(listenPortStr.split("-")[1]);
        } else if(listenPortStr.indexOf(":")>-1){
            listenPort = Integer.parseInt(listenPortStr.split(":")[0]);
            listenPortMax = Integer.parseInt(listenPortStr.split(":")[1]);
        } else {
            listenPort = Integer.parseInt(listenPortStr);
            listenPortMax = 0;
        }
        if(proxyType>1){
            if(listenPortMax>0){
                for (int i = listenPort; i<= listenPortMax; i++){
                    deleteLink(i, 0);
                    deleteLink(i, 1);
                }
            } else {
                deleteLink(listenPort, 0);
                deleteLink(listenPort, 1);
            }
        } else {
            if(listenPortMax>0){
                for (int i = listenPort; i<= listenPortMax; i++){
                    deleteLink(i, proxyType);
                }
            } else {
                deleteLink(listenPort, proxyType);
            }
        }
    }
}
