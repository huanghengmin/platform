package com.hzih.audit;

/**
 * Created by Administrator on 15-11-6.
 */
public class AuditProcess {

    private String errorSqlInsert = "insert into business_db_error_log " +
            "(log_time,level,business_name,source_jdbc_name,source_table_name," +
            "target_jdbc_name,target_table_name,pk_value,audit_error,operator,flag) " +
            "values " +
            "(?,?,?,?,?,?,?,?,?,?,0)";

    private String fluxSqlInsert = "insert into business_db_flux " +
            "(business_name,source_jdbc_name,source_table_name," +
            "target_jdbc_name,target_table_name,cycle,count_i,count_d,count_u,flag) " +
            "values " +
            "(?,?,?,?,?,?,?,?,?,?,0)";

    private String fluxSqlUpdate = "update business_db_flux set ";
}
