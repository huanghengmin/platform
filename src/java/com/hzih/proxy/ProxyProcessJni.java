package com.hzih.proxy;

import org.apache.log4j.Logger;

import java.io.File;
import java.lang.reflect.Method;

/**
 * Created by 钱晓盼 on 15-1-4.
 */
public class ProxyProcessJni {
    final static Logger logger = Logger.getLogger(ProxyProcessJni.class);


    static {
        Method llm;
        try{
            llm = ClassLoader.class.getDeclaredMethod("loadLibrary0", Class.class, File.class);
            llm.setAccessible(true);
            llm.invoke(null,new Object[]{ProxyProcessJni.class,new File(System.getProperty("ichange.home")+"/others/libPortProxyMap.so")});
        } catch (Exception e) {
            logger.error("加载libPortMap.so失败",e);
        }
    }

    /**
     * 创建一条转发规则
     * @param listenHost 监听地址
     * @param listenPort    监听端口
     * @param dstHost   目标地址
     * @param dstPort   目标端口
     * @param timeout   超时时间
     * @param proxyType 服务类型0:tcp,1:udp
     * @return
     */
    private native int add_tunnel(
                                  String listenHost,int listenPort,
                                  String dstHost,int dstPort,
                                  int timeout,int proxyType);

    /**
     * 删除一条规则
     * @param listenPort 新增中的监听端口
     * @param proxyType 服务类型0:tcp,1:udp
     * @return
     */
    private native int del_tunnel(int listenPort,int proxyType);

    /**
     * 查询流量
     * @param listenPort 新增中的监听端口
     * @param proxyType 服务类型0:tcp,1:udp
     * @return
     */
    private native double[] get_traffic(int listenPort,int proxyType);


    public void addLink(String appName,
                           String listenHost,int listenPort,
                           String dstHost,int dstPort,
                           int timeout,int proxyType) {
        int code = add_tunnel(
                listenHost, listenPort,
                dstHost, dstPort, timeout, proxyType
        );
        String resultStr = appName+ "应用启动"+(proxyType==0?"TCP":"UDP")+"通道"+listenPort;
        result(code,resultStr);

    }



    public void deleteLink(String appName,int listenPort,int proxyType) {
        int code = del_tunnel(listenPort, proxyType);
        String resultStr = appName+"停止"+(proxyType==0?"TCP":"UDP")+"通道"+listenPort;
        result(code,resultStr);
    }

    public double[] getFlux(int listenPort,int proxyType) {
        double[] flux = get_traffic(listenPort, proxyType);
        if(flux == null) {
            flux = new double[]{0.0,0.0};//发送,接收
        }
        return flux;
    }

    private void result(int code,String resultStr) {
        switch (code) {
            case 0:
                logger.info(resultStr +" 成功,返回码: " + code);
                break;
            case -1:
                logger.info(resultStr +" 失败,错误码: " + code);
                break;
            case -2:
                logger.info(resultStr +" 创建监听套接字失败,错误码: " + code);
                break;
            case -3:
                logger.info(resultStr +" 绑定监听地址失败,错误码: " + code);
                break;
            case -4:
                logger.info(resultStr +" 创建转发套接字失败,错误码: " + code);
                break;
            case -5:
                logger.info(resultStr +" 转发套接字绑定地址失败,错误码: " + code);
                break;
            case -6:
                logger.info(resultStr +" connect失败,错误码: " + code);
                break;
            case -7:
                logger.info(resultStr +" 内存分配失败,错误码: " + code);
                break;
            case -8:
                logger.info(resultStr +" 初始化链表失败,错误码: " + code);
                break;
            case -9:
                logger.info(resultStr +" 线程信息插入链表失败,错误码: " + code);
                break;
            case -10:
                logger.info(resultStr +" 创建线程失败,错误码: " + code);
                break;
            case -11:
                logger.info(resultStr +" 没有找到要删除相应的端口,错误码: " + code);
                break;
            case -12:
                logger.info(resultStr +" 链表未初始化,错误码: " + code);
                break;
            default:
                logger.info(resultStr +" 失败,错误码: " + code);

        }
    }

//    public static void main(String[] args) {
//        System.setProperty("ichange.home","/usr/app/ichange");
//        System.out.println("请输入来源IP:");
//        Scanner scanner = new Scanner(System.in);
//        String srcHost = scanner.next();
//        System.out.println("请输入来源端口:");
//        int srcPort = Integer.parseInt(scanner.next());
//        System.out.println("请输入监听IP:");
//        String listenHost = scanner.next();
//        System.out.println("请输入监听端口");
//        int listenPort = Integer.parseInt(scanner.next());
//        System.out.println("请输入发送IP:");
//        String sendHost = scanner.next();
//        System.out.println("请输入发送端口:");
//        int sendPort = Integer.parseInt(scanner.next());
//        System.out.println("请输入目标IP:");
//        String dstHost = scanner.next();
//        System.out.println("请输入目标端口:");
//        int dstPort = Integer.parseInt(scanner.next());
//        System.out.println("请输入开启时间长短:");
//        int num = Integer.parseInt(scanner.next());
//
//        VideoProcessJni videoProcessJni = new VideoProcessJni();
//        videoProcessJni.add_thread(
//                srcHost,srcPort,
//                listenHost,listenPort,
//                sendHost,sendPort,
//                dstHost,dstPort,num
//                );
//        for (int i=0; i < 100; i++) {
//            try {
//                Thread.sleep(1000 * 10);
//            } catch (InterruptedException e) {
//            }
//            double[] fluxs = videoProcessJni.check_traffic(listenPort);
//            System.out.println("监听网卡上行/下行:"+fluxs[0]+"/"+fluxs[2]);
//            System.out.println("发送网卡上行/下行:"+fluxs[1]+"/"+fluxs[3]);
//        }
//        videoProcessJni.del_thread(listenPort);
//        System.exit(1);
//    }
}
