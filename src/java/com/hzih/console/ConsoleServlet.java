package com.hzih.console;

import com.hzih.utils.PlatformUtils;
import com.hzih.db.entity.Type;
import com.hzih.db.utils.Configuration;
import com.hzih.jdbc.DataSourceUtil;
import com.hzih.platform.DBService;
import com.hzih.platform.ProxyService;
import com.hzih.platform.Service;
import com.inetec.common.config.ConfigParser;
import com.inetec.common.config.nodes.IChange;
import com.inetec.common.config.nodes.Jdbc;
import org.apache.log4j.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import java.io.IOException;

/**
 * Created by Administrator on 15-6-26.
 */
public class ConsoleServlet extends HttpServlet {

    private final static Logger logger = Logger.getLogger(ConsoleServlet.class);
    public static final String configXml = System.getProperty("ichange.home")+"/repository/config.xml";
//    public static final String configXml = System.getProperty("ichange.home")+"/repository/config_db.xml";
    public static Service service;
    public static ProxyService proxyService;
    public static DataSource localDataSource;

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) {
        String commandType = req.getParameter("commandType");//app or jdbc
        if("app".equalsIgnoreCase(commandType)) {
            String appName = req.getParameter("appName");
//          appName = "db_1_2";
            String appType = req.getParameter("appType");
            if(Type.s_app_proxy.equalsIgnoreCase(appType)) {
                try{
                    if(proxyService == null){
                        proxyService = new ProxyService();
                        proxyService.init(appName);
                    }
                    if(!proxyService.isRun()){
                        new Thread(proxyService).start();
                    }
                    String command = req.getParameter("command");//start,stop
                    if(command != null){
                        String param = req.getParameter("param");
                        if("start".equalsIgnoreCase(command)){
                            proxyService.startLink(param);//192.168.1.1|8080:8090|172.16.1.1|8080:8090|60|2
                            resp.getWriter().write("应用 " + appName + " 打开代理" + param + "执行成功");
                        } else if("stop".equalsIgnoreCase(command)){
                            proxyService.stopLink(param);//192.168.1.1|8080:8090|2
                            resp.getWriter().write("应用 " + appName + " 停止代理"+param+"行成功");
                        } else {
                            resp.getWriter().write("应用 " + appName + " 接收未知命令"+command);
                        }
                    }
                    resp.setStatus(HttpServletResponse.SC_OK);
                } catch (Exception e) {
                    logger.error("应用"+appName+"接收命令后处理失败"+e.getMessage(),e);
                    resp.setStatus(HttpServletResponse.SC_EXPECTATION_FAILED);
                }
            } else {
                String command = req.getParameter("command");//start,stop,status
                try{
                    if(service!=null&&service.isRun()) {
                        resp.setCharacterEncoding("gbk");
                        if("stop".equalsIgnoreCase(command)){
                            service.stopThread();
                            resp.getWriter().write("应用 " + appName + " 停止命令执行成功");
                            resp.setStatus(HttpServletResponse.SC_OK);
                        } else if("status".equalsIgnoreCase(command)){
                            if(!service.isOperatorStopped()){
                                resp.getWriter().write("应用 " + appName + " 停止成功");
                                resp.setStatus(HttpServletResponse.SC_OK);
                            } else {
                                resp.getWriter().write("应用 " + appName + " 正在停止中");
                                resp.setStatus(HttpServletResponse.SC_SEE_OTHER);
                            }
                        } else if("start".equalsIgnoreCase(command)){
                            resp.getWriter().write("应用 " + appName + " 已经在运行");
                            resp.setStatus(HttpServletResponse.SC_OK);
                        } else {
                            resp.getWriter().write("应用 " + appName + " 不能识别命令" + command);
                            resp.setStatus(HttpServletResponse.SC_SEE_OTHER);
                        }
                    } else {
                        ConfigParser configParser = new ConfigParser(configXml);
                        IChange iChange = configParser.getRoot();
                        Type type = Configuration.getType(appName,configXml);
                        if(appType.startsWith(Type.s_app_db)){
                            service = new DBService();
                            String sourceJdbcName = type.getSourceDB();
                            String targetJdbcName = type.getTargetDB();
                            Jdbc sourceJdbc = iChange.getJdbc(sourceJdbcName);
                            Jdbc targetJdbc = iChange.getJdbc(targetJdbcName);
                            service.init(type,sourceJdbc,targetJdbc);
                            new Thread(service).start();
                        } else if(Type.s_app_sipproxy.equalsIgnoreCase(appType)) {
                        } else if(Type.s_app_file.equalsIgnoreCase(appType)) {
                        } else {
                        }

                        resp.setCharacterEncoding("gbk");
                        logger.info("应用 " + appName + " 启动成功");
                        resp.getWriter().write("应用 " + appName + " 启动成功");
                        resp.setStatus(HttpServletResponse.SC_OK);
                    }
                } catch (Exception e) {
                    logger.error("应用" + appName + "启动错误", e);
                    resp.setStatus(HttpServletResponse.SC_EXPECTATION_FAILED);
                }

            }
        } else if("jdbc".equalsIgnoreCase(commandType)) {
            String jdbcName = req.getParameter("jdbcName");
            try{
                ConfigParser configParser = new ConfigParser(configXml);
                IChange iChange = configParser.getRoot();
                Jdbc jdbc = iChange.getJdbc(jdbcName);
                resp.setCharacterEncoding("gbk");
                if(jdbc == null) {
                    resp.getWriter().write("加载 " + jdbcName + " 失败,配置中不存在");
                    resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
                } else {
                    DataSourceUtil.local = PlatformUtils.jdbcToMap(jdbc);
                    ConsoleServlet.localDataSource = DataSourceUtil.getDataSource(DataSourceUtil.DRUID_LOCAL);
                    resp.getWriter().write("加载 " + jdbcName + " 成功");
                    resp.setStatus(HttpServletResponse.SC_OK);
                }
            } catch (Exception e) {
                logger.error("加载" + jdbcName + "错误", e);
                resp.setStatus(HttpServletResponse.SC_EXPECTATION_FAILED);
            }
        }
    }

    private void start(String appName) {

    }

    private void stop(String appName) {

    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req,resp);
    }



}
