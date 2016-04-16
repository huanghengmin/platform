package com.hzih.http;

import junit.framework.TestCase;

/**
 * Created by 钱晓盼 on 15-11-6.
 */
public class TestHttp extends TestCase {


    public void testManager(){

        String appName = "proxy_2";
        String appType = "proxy";
        String jdbcName = "local_mysql";
        String managerUrl = "http://172.16.3.221:8060/manager/ManagerServer";
        String[][] params = controlManager(appName, appType, "create");//创建应用
//        String[][] params = controlManager(appName, appType, "start");//启动应用 (每次重启后需要再次发送一遍)
//        String[][] params = controlManager(appName, appType, "stop");//停止应用 (代理不需要发送)
//        String[][] params = controlManager(appName, appType, "destroy");//删除应用
//        String[][] params = loadJdbc(jdbcName,appName,appType);//给应用加载jdbc(mysql数据库)
//        String[][] params = controlProxyApp(appName,appType,"start","172.16.3.221|20001:20005|192.168.1.94|20001:20005|60|2");//启动代理端口
//        String[][] params = controlProxyApp(appName,appType,"stop","172.16.3.221|20001:20005|192.168.1.94|20001:20005|60|2");//停止代理端口

        ServiceResponse response = ServiceUtil.callManager(params,managerUrl);
        if (response != null && response.getData() != null) {
            System.out.println( "返回码: " + response.getCode() + "  内容 : " + new String(response.getData()));
        } else {
            System.out.println( "返回码: " + response.getCode());
        }


    }

    /**
     * 启停应用, 添加代理端口
     * @param appName
     * @param appType   db file proxy sipproxy
     * @param command          start                                                        stop
     * @param param     代理: 192.168.1.1|8080:8090|172.16.1.1|8080:8090|60|2       192.168.1.1|8080:8090|2
     * @return
     */
    private String[][] controlProxyApp(String appName,String appType,String command,String param) {
        String[][] params = new String[][] {
                { "commandType", "app" },
                { "appName", appName },
                { "appType", appType },
                { "command", command },
                { "param", param }
        };
        return params;
    }

    /**
     * 应用启动前给启动的容器添加本地数据库的数据源
     * @param jdbcName
     * @return
     */
    private String[][] loadJdbc(String jdbcName,String appName,String appType) {
        String[][] params = new String[][] {
                { "commandType", "jdbc" },
                { "command", "jdbc" },
                { "jdbcName", jdbcName },
                { "appType", appType },
                { "appName", appName }
        };

        return new String[0][];
    }


    /**
     * 应用保存的最后一步,创建运行容器
     * @param appName
     * @param appType  db file proxy sipproxy
     * @param command  create stop or destroy
     * @return
     */
    public String[][] controlManager(String appName,String appType,String command){
        String[][] params = new String[][] {
                { "commandType", "app" },
                { "appName", appName },
                { "appType", appType },
                { "command", command }
        };
        return params;
    }



}
