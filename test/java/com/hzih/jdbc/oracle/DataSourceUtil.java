package com.hzih.jdbc.oracle;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.inetec.common.config.nodes.Jdbc;
import com.inetec.common.security.DesEncrypterAsPassword;
import org.apache.log4j.Logger;

/**
 * The Class DataSourceUtil.
 */
public class DataSourceUtil {

    private final static Logger logger = Logger.getLogger(DataSourceUtil.class);
    public static Map<String,Map<String,String>> jdbcMap = new HashMap<String,Map<String,String>>();
    public static Map<String,DataSource> dataSourceMap = new HashMap<String, DataSource>();

    /**
     * 获取数据源
     * @param appName
     * @param jdbc
     * @return
     * @throws Exception
     */
    public static DataSource getDataSource(String appName, Jdbc jdbc) throws Exception {
        DataSource dataSource = dataSourceMap.get(jdbc.getJdbcName());
        if(dataSource == null){
            Map<String,String> map = jdbcMap.get(jdbc.getJdbcName());
            if(map == null){
                map = jdbcToMap(jdbc);
            }
            jdbcMap.put(jdbc.getJdbcName(),map);
            dataSource = DruidDataSourceFactory.createDataSource(map);
            dataSourceMap.put(jdbc.getJdbcName(),dataSource);
        }
        return dataSource;
    }

    /**
     * 更新数据源
     * @param appName
     * @param jdbc
     */
    public static void refreshSource(String appName, Jdbc jdbc){
        Map<String,String> map = jdbcMap.get(appName);
        if(map!=null){
            jdbcMap.remove(appName);
        }
        Map<String,String> mapNew = jdbcToMap(jdbc);
        jdbcMap.put(appName,map);
    }

    private static Map<String,String> jdbcToMap(Jdbc jdbc){
        Map<String,String> source = new HashMap<String,String>();
        source.put(DruidDataSourceFactory.PROP_DRIVERCLASSNAME,jdbc.getDriverClass());
        source.put(DruidDataSourceFactory.PROP_URL,jdbc.getDbUrl());
        source.put(DruidDataSourceFactory.PROP_USERNAME,jdbc.getDbUser());
        String password = null;
        try{
            DesEncrypterAsPassword deap = new DesEncrypterAsPassword("inetec~!@#$%^&*()_+");
            password = new String(deap.decrypt(jdbc.getPassword().getBytes()));
        } catch (Exception e){
            logger.error("jdbc "+jdbc.getJdbcName()+" 密码解密错误",e);
        }
        source.put(DruidDataSourceFactory.PROP_PASSWORD,password);
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

