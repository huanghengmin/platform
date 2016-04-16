package com.hzih.audit;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.hzih.jdbc.DataSourceUtil;
import com.inetec.common.config.ConfigParser;
import com.inetec.common.config.nodes.IChange;
import com.inetec.common.config.nodes.Jdbc;
import junit.framework.TestCase;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 15-11-6.
 */
public class TestAuditProcess extends TestCase {
    private final static Logger logger = Logger.getLogger(TestAuditProcess.class);
    public static final String configXml = System.getProperty("ichange.home")+"/repository/config.xml";

    public void testDbAudit(){
        DataSource dataSource = getLocalDataSource();
        String Sql_Insert_Business_Db_Flux =
                "insert into business_db_flux (" +
                        "business_name," +
                        "source_jdbc_name," +
                        "source_table_name," +
                        "target_jdbc_name," +
                        "target_table_name," +
                        "cycle," +
                        "count_i," +
                        "count_d," +
                        "count_u," +
                        "flag" +
                        ") values (?,?,?,?,?,?,?,?,?,?)";
        String Sql_Update_Business_Db_Flux =
                "update business_db_flux set " +
                        "cycle = ?," +
                        "count_i = ?," +
                        "count_d = ?," +
                        "count_u = ?" +
                        " where business_name = ? and" +
                        " source_jdbc_name = ? and" +
                        " source_table_name = ? and" +
                        " target_jdbc_name = ? and" +
                        " target_table_name = ?";
        String Sql_Insert_Business_Db_Error_Log =
                "insert into business_db_error_log (" +
                        "log_time," +
                        "level," +
                        "business_name," +
                        "source_jdbc_name," +
                        "source_table_name," +
                        "target_jdbc_name," +
                        "target_table_name," +
                        "pk_value," +
                        "audit_error," +
                        "operator," +
                        "flag" +
                        ") values (?,?,?,?,?,?,?,?,?,?,?)";


    }

    public DataSource getLocalDataSource(){
        String jdbcName = "local_mysql";
        DataSource dataSource = null;
        try{
            ConfigParser configParser = new ConfigParser(configXml);
            IChange iChange = configParser.getRoot();
            Jdbc jdbc = iChange.getJdbc(jdbcName);
            if(jdbc==null) {
                logger.info("加载 " + jdbcName + " 失败,配置中不存在");
            } else {
                DataSourceUtil.local = jdbcToMap(jdbc);
                dataSource = DataSourceUtil.getDataSource(DataSourceUtil.DRUID_LOCAL);
                logger.info("加载 " + jdbcName + " 成功");
            }
        } catch (Exception e) {
            logger.error("加载" + jdbcName + "错误", e);
        }
        return dataSource;
    }

    private Map<String,String> jdbcToMap(Jdbc jdbc){
        Map<String,String> source = new HashMap<String,String>();
        source.put(DruidDataSourceFactory.PROP_DRIVERCLASSNAME,jdbc.getDriverClass());
        source.put(DruidDataSourceFactory.PROP_URL,jdbc.getDbUrl());
        source.put(DruidDataSourceFactory.PROP_USERNAME,jdbc.getDbUser());
        source.put(DruidDataSourceFactory.PROP_PASSWORD,jdbc.getPassword());
        source.put(DruidDataSourceFactory.PROP_FILTERS,"stat");
        source.put(DruidDataSourceFactory.PROP_INITIALSIZE,"2");
        source.put(DruidDataSourceFactory.PROP_MAXACTIVE,"300");
        source.put(DruidDataSourceFactory.PROP_MAXWAIT,"60000");
        source.put(DruidDataSourceFactory.PROP_TIMEBETWEENEVICTIONRUNSMILLIS,"60000");
        source.put(DruidDataSourceFactory.PROP_MINEVICTABLEIDLETIMEMILLIS,"300000");
        source.put(DruidDataSourceFactory.PROP_VALIDATIONQUERY,"SELECT 1 FROM DUAL");
        source.put(DruidDataSourceFactory.PROP_TESTWHILEIDLE,"true");
        source.put(DruidDataSourceFactory.PROP_TESTONBORROW,"false");
        source.put(DruidDataSourceFactory.PROP_TESTONRETURN,"false");
        source.put(DruidDataSourceFactory.PROP_POOLPREPAREDSTATEMENTS,"false");
        source.put("maxPoolPreparedStatementPerConnectionSize","200");
        return source;
    }
}
