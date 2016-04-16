package com.hzih.jdbc;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSourceFactory;

/**
 * The Class DataSourceUtil.
 */
public class DataSourceUtil {

    /** 使用配置文件构建Druid数据源. */
    public static final int DRUID_SOURCE = 0;

    /** 使用配置文件构建Druid数据源. */
    public static final int DRUID_TARGET = 1;

    /** 使用配置文件构建Druid数据源. */
    public static final int DRUID_LOCAL = 2;

    /** 源数据源 */
    public static Map<String,String> source = new HashMap<String,String>();
    /** 目标数据源 */
    public static Map<String,String> target = new HashMap<String,String>();
    /** 本地数据源 */
    public static Map<String,String> local = new HashMap<String,String>();

    /**
     * 根据类型获取数据源
     *
     * @param sourceType
     *            数据源类型
     * @return druid或者dbcp数据源
     * @throws Exception
     *             the exception
     */
    public static final DataSource getDataSource(int sourceType) throws Exception {
        DataSource dataSource = null;
        switch (sourceType) {
            case DRUID_SOURCE:
                dataSource = DruidDataSourceFactory.createDataSource(source);
                break;
            case DRUID_TARGET:
                dataSource = DruidDataSourceFactory.createDataSource(target);
                break;
            case DRUID_LOCAL:
                dataSource = DruidDataSourceFactory.createDataSource(local);
                break;
        }
        return dataSource;
    }
}

