package com.hzih.console;

import com.hzih.utils.PlatformUtils;
import com.hzih.db.entity.Type;
import com.hzih.db.utils.Configuration;
import com.hzih.jdbc.DataSourceUtil;
import com.hzih.platform.DBService;
import com.hzih.platform.Service;
import com.inetec.common.config.ConfigParser;
import com.inetec.common.config.nodes.IChange;
import com.inetec.common.config.nodes.Jdbc;
import junit.framework.TestCase;
import org.apache.commons.io.IOUtils;

import javax.sql.DataSource;
import java.io.*;
import java.sql.*;

/**
 * Created by Administrator on 15-10-29.
 */
public class Test extends TestCase {

    public void testMain() {
        System.setProperty("ichange.home","D:/app/ichange");
        Service service = null;
        try{
//            String appName = "db_1_1";
            String appName = "qbtb";
//            String appName = "xltest";
//            String appName = "cftb";
//            String appName = "sxtb";
//            String appName = "db_1_3";
//            String appName = "db_1_4";
            if(service!=null&&service.isRun()) {

            } else {
                ConfigParser configParser = new ConfigParser(ConsoleServlet.configXml);
                IChange iChange = configParser.getRoot();
                Type type = Configuration.getType(appName,ConsoleServlet.configXml);
                String appType = "db";
                if(Type.s_app_db.equalsIgnoreCase(appType)){
                    service = new DBService();
                    String sourceJdbcName = type.getSourceDB();
                    String targetJdbcName = type.getTargetDB();
                    Jdbc sourceJdbc = iChange.getJdbc(sourceJdbcName);
                    Jdbc targetJdbc = iChange.getJdbc(targetJdbcName);
                    service.init(type,sourceJdbc,targetJdbc);
                } else if(Type.s_app_sipproxy.equalsIgnoreCase(appType)) {
                } else if(Type.s_app_proxy.equalsIgnoreCase(appType)) {
                } else if(Type.s_app_file.equalsIgnoreCase(appType)) {

                } else {

                }
                new Thread(service).start();
//                resp.setCharacterEncoding("gbk");
//                resp.getWriter().write("应用 " + appName + " 启动成功");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        while (true){
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
            }
        }
    }


    public void testLob() {
        System.setProperty("ichange.home","D:/app/ichange");
        Service service = null;
        try{
            String appName = "qbtb";
            if(service!=null&&service.isRun()) {

            } else {
                ConfigParser configParser = new ConfigParser(ConsoleServlet.configXml);
                IChange iChange = configParser.getRoot();
                Type type = Configuration.getType(appName,ConsoleServlet.configXml);
                String appType = type.getAppType();
                if(Type.s_app_db.equalsIgnoreCase(appType)|| "dbOneLine".equalsIgnoreCase(appType)){
                    service = new DBService();
                    String sourceJdbcName = type.getSourceDB();
                    String targetJdbcName = type.getTargetDB();
                    Jdbc sourceJdbc = iChange.getJdbc(sourceJdbcName);
                    Jdbc targetJdbc = iChange.getJdbc(targetJdbcName);
                    DataSourceUtil.source = PlatformUtils.jdbcToMap(sourceJdbc);

                    blob();



                } else if(Type.s_app_sipproxy.equalsIgnoreCase(appType)) {
                } else if(Type.s_app_proxy.equalsIgnoreCase(appType)) {
                } else if(Type.s_app_file.equalsIgnoreCase(appType)) {

                } else {

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private void blob() {
        Connection conn = null;
        PreparedStatement prepStmt = null;
        DataSource dataSource = null;
        try {
            dataSource = DataSourceUtil.getDataSource(DataSourceUtil.DRUID_SOURCE);
        } catch (Exception e) {
            e.printStackTrace();
        }
        int id = 1;
        try{
//            File f = new File("D:/test/cpu.txt");
            File f = new File("D:/test/0013050_600_375.jpg");

            FileInputStream fis = new FileInputStream(f);
            byte[] data=null;
            data = IOUtils.toByteArray(fis);
            fis.close();
            conn = dataSource.getConnection();
            prepStmt = conn.prepareStatement("insert into test_db_2 (id,test1,test2,test3,test4) values (?,?,?,?,?)");
            prepStmt.setInt(1, id);
            prepStmt.setString(2, "lob 测试");
            prepStmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            prepStmt.setObject(4, data);
//            prepStmt.setBinaryStream(4,fis);
            prepStmt.setString(5, "45454545454545");
            prepStmt.addBatch();
//            fis.close();
//            fis = new FileInputStream(f);
            id ++;
            prepStmt.setInt(1, id);
            prepStmt.setString(2, "lob 测试");
            prepStmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            prepStmt.setObject(4, data);
//            prepStmt.setBinaryStream(4,fis);
            prepStmt.setString(5, "45454545454545");
            prepStmt.addBatch();
//            prepStmt.executeUpdate();
            prepStmt.executeBatch();
            prepStmt.clearBatch();
            prepStmt.close();


            conn.setAutoCommit(false);
            prepStmt = conn.prepareStatement("select test3,test4 from test_db_2 where id = ? for update ");

            prepStmt.setInt(1,id);
            ResultSet rs = prepStmt.executeQuery();
            while (rs.next()){
                Blob blob = rs.getBlob("test3");

                InputStream ins = blob.getBinaryStream();
                //输出到文件
//                File file = new File("d:/test/cpu(1).txt");
                File file = new File("d:/test/0013050_600_375(1).jpg");
                OutputStream fout = new FileOutputStream(file);
                //下面将BLOB数据写入文件
                byte[] b = new byte[1024];
                int len = 0;
                while ( (len = ins.read(b)) != -1) {
                    fout.write(b, 0, len);
                }
                //依次关闭
                fout.close();
                ins.close();

                String str = rs.getString("test4");
                Clob clob = rs.getClob("test4");
                Reader reader = clob.getCharacterStream();
                if(reader != null) {
                    StringBuffer sb = new StringBuffer();
                    char[] charbuff = new char[4096];
                    for(int i = reader.read(charbuff);i>0;i=reader.read(charbuff)) {
                        sb.append(charbuff,0,i);
                    }
                    System.out.println(sb.toString());
                }





//                BLOB blob = (BLOB) rs.getBlob("TEST3");
//                OutputStream os = blob.getBinaryOutputStream();
//                BufferedOutputStream output = new BufferedOutputStream(os);
//                BufferedInputStream input = new BufferedInputStream(new FileInputStream(file));
//
//                byte[] buff = new byte[2048];
//                int len = 0;
//                while ((len=input.read())!=-1){
//                    output.write(buff,0,len);
//                }
//                output.flush();
//                output.close();
//                input.close();
//
//                Clob clob = rs.getClob("TEST4");
//                OutputStream os_c = clob.setAsciiStream(2048);
//                BufferedOutputStream output_c = new BufferedOutputStream(os_c);
//                BufferedInputStream input_c = new BufferedInputStream(new FileInputStream(file2));
//                byte[] buff_c = new byte[2048];
//                int len_c = 0;
//                while ((len_c=input_c.read())!=-1){
//                    output_c.write(buff_c,0,len_c);
//                }
//                output_c.flush();
//                output_c.close();
//                input_c.close();
            }

            conn.commit();

            System.out.println("完成lob入库");

        } catch (Exception e){
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (prepStmt != null) {
                try {
                    prepStmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}
