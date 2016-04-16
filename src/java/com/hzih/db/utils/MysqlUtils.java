package com.hzih.db.utils;

import com.hzih.db.entity.FieldValue;
import net.sf.json.JSONObject;
import oracle.jdbc.OracleConnection;
import oracle.spatial.geometry.JGeometry;
import oracle.sql.STRUCT;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Date;

/**
 * Created by Administrator on 15-11-9.
 */
public class MysqlUtils implements IDBUtils{

    private final static Logger logger = Logger.getLogger(MysqlUtils.class);

    public final static String[][] oracle2jdbc = {
            {"CHAR", "CHAR"},
            {"VARCHAR2", "VARCHAR"},
            {"LONG", "LONGVARCHAR"},
            {"NUMBER", "NUMERIC"},
            {"RAW", "VARBINARY"},
            {"LONGRAW", "LONGVARBINARY"},
            {"DATE", "DATE"},
            {"TIME", "TIME"},
            {"TIMESTAMP", "TIMESTAMP"},
            {"BLOB", "BLOB"},
            {"CLOB", "CLOB"},
            {"SDO_GEOMETRY", "STRUCT"}
    };

    public int getJdbcTypeFromVenderDb(String type) {
        int size = oracle2jdbc.length;
        type.trim();
        if (type.indexOf("(") > 0) {
            type = type.substring(0, type.indexOf("("));
        }
        for (int i = 0; i < size; i++) {
            if (type.equalsIgnoreCase(oracle2jdbc[i][0])) {
                return DBUtils.getJdbcType(oracle2jdbc[i][1]);
            }
        }
        return Types.VARCHAR;
    }


    @Override
    public String createSql_SelectFromTemp(String tempTable,String sourceTableName,int maxRecords){
        String Sql_SelectFromTemp;
        if(sourceTableName!=null){
            Sql_SelectFromTemp = "select id,dbname,tablename, pks, op,op_time from " +
                    "( select * from " + tempTable + " where id > 0 " +
                    "and tablename='"+sourceTableName.toUpperCase()+"' order by id asc) tmp where rownum <= " + maxRecords;
        } else {
            Sql_SelectFromTemp = "select id,dbname,tablename, pks, op,op_time from " +
                "( select * from " + tempTable + " where id > 0 " +
                "order by id asc) tmp where rownum <= " + maxRecords;
        }
        return Sql_SelectFromTemp;
    }

    @Override
    public String createSql_Where_pk_Twoway(String appName, String dbType, String fieldType, String pk) {
        if("NUMBER".equalsIgnoreCase(fieldType)){
        } else if("VARCHAR2".equalsIgnoreCase(fieldType)){
            pk = "'"+pk+"'";
        } else if("DATE".equalsIgnoreCase(fieldType)){
            pk = "to_date('"+pk+"','yyyy-mm-dd hh24:mi:ss')";
        } else if("TIMESTAMP(6)".equalsIgnoreCase(fieldType)){
            pk = "to_timestamp('" + pk + "','yyyy-mm-dd hh24:mi:ss')";
        } else {
            logger.warn(appName + " 暂时不支持使用"+fieldType+"类型的主键处理");
        }
        int jdbcTypeInt = getJdbcTypeFromVenderDb(fieldType);
        switch (jdbcTypeInt) {
            case Types.BIT:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.REAL:        //not specified
            case Types.DOUBLE:
            case Types.DECIMAL:     //not specified
            case Types.NUMERIC:
                break;
            case Types.CHAR:        //not specified
            case Types.VARCHAR:
                pk = "'"+pk+"'";
                break;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                pk = "to_timestamp('" + pk + "','yyyy-mm-dd hh24:mi:ss')";
                break;
            case Types.BLOB:
            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
            case Types.CLOB:
            case Types.LONGVARCHAR:
            case Types.NULL:
            case Types.DISTINCT:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.ARRAY:
            case Types.REF:
            case Types.STRUCT:
            default:
                logger.warn(appName + " 暂时不支持使用"+fieldType+"类型的主键处理");
                break;
        }
        return pk;
    }


    @Override
    public PreparedStatement setPrepareStatement (int idx,FieldValue fieldValue,
                                                  Connection connTarget, PreparedStatement prepStmt) throws SQLException {
        String jdbcType = fieldValue.getJdbcType();
        int jdbcTypeInt = DBUtils.getJdbcType(jdbcType);
        Object obj = fieldValue.getObj();
        switch (jdbcTypeInt) {
            case Types.BIT:
                Boolean bit = (Boolean) obj;
                prepStmt.setBoolean(idx, bit);
                break;
            case Types.TINYINT:
                Byte tinyint = (Byte) obj;
                prepStmt.setByte(idx,tinyint);
                break;
            case Types.SMALLINT:
                Short smallint = (Short) obj;
                prepStmt.setShort(idx,smallint);
                break;
            case Types.INTEGER:
                Integer integer = (Integer) obj;
                prepStmt.setInt(idx, integer);
                break;
            case Types.BIGINT:
                Long bigint = (Long) obj;
                prepStmt.setLong(idx,bigint);
                break;
            case Types.FLOAT:
                Float aFloat = (Float) obj;
                prepStmt.setFloat(idx, aFloat);
                break;
            case Types.REAL:        //not specified
            case Types.DOUBLE:
                Double aDouble = (Double) obj;
                prepStmt.setDouble(idx,aDouble);
                break;
            case Types.DECIMAL:     //not specified
            case Types.NUMERIC:
                BigDecimal numeric = (BigDecimal) obj;
                prepStmt.setBigDecimal(idx, numeric);
                break;
            case Types.CHAR:        //not specified
            case Types.VARCHAR:
                String str = (String) obj;
                prepStmt.setString(idx,str);
                break;
            case Types.DATE:
                Date date = (Date) obj;
                prepStmt.setTimestamp(idx, (Timestamp) date);
                break;
            case Types.TIME:
                prepStmt.setTime(idx, (Time) obj);
                break;
            case Types.TIMESTAMP:
                Timestamp timestamp = (Timestamp) obj;
                prepStmt.setTimestamp(idx, timestamp);
                break;
            case Types.BLOB:
            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
                byte[] data = (byte[]) obj;
                prepStmt.setObject(idx, data);
                break;
            case Types.CLOB:
            case Types.LONGVARCHAR:
                String clob = obj == null?"":(String) obj;
                prepStmt.setString(idx, clob);
                break;
            case Types.NULL:
            case Types.DISTINCT:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
                prepStmt.setObject(idx,obj);
                break;
            case Types.ARRAY:
                prepStmt.setArray(idx, (Array) obj);
                break;
            case Types.REF:
                prepStmt.setRef(idx, (Ref) obj);
                break;
            case Types.STRUCT:
                if(connTarget!=null){
                    String valueString = (String) obj;
                    JSONObject _obj = JSONObject.fromObject(valueString);
                    int gtype = _obj.getInt("g");
                    int srid = _obj.getInt("s");
                    double x = _obj.getDouble("x");
                    double y = _obj.getDouble("y");
                    double z = _obj.getDouble("z");
                    int[] elemInfo = MathUtils.getIntegerArray(_obj.getString("e"));
                    double[] ordinate = MathUtils.getDoubleArray(_obj.getString("o"));
                    STRUCT struct = MyJGeometry.restore(gtype, srid, x, y, z, elemInfo, ordinate, connTarget);
                    prepStmt.setObject(idx,struct);
                } else {
                    prepStmt.setObject(idx,null);
                }
                break;
            default:
                logger.warn("ORACLE未实现的字段类型"+fieldValue.getDbType());
                break;
        }
        return prepStmt;
    }


    @Override
    public PreparedStatement setPrepareStatementPk(int idx, String fieldType,
                                                          String pkName, String pkStr, PreparedStatement prepStmt) throws SQLException {
        int jdbcTypeInt = getJdbcTypeFromVenderDb(fieldType);
        switch (jdbcTypeInt) {
            case Types.BIT:
                Boolean bit = Boolean.valueOf(pkStr);
                prepStmt.setBoolean(idx, bit);
                break;
            case Types.TINYINT:
                Byte tinyint = Byte.valueOf(pkStr);
                prepStmt.setByte(idx,tinyint);
                break;
            case Types.SMALLINT:
                Short smallint = Short.valueOf(pkStr);
                prepStmt.setShort(idx,smallint);
                break;
            case Types.INTEGER:
                Integer integer = Integer.valueOf(pkStr);
                prepStmt.setInt(idx, integer);
                break;
            case Types.BIGINT:
                Long bigint = Long.valueOf(pkStr);
                prepStmt.setLong(idx,bigint);
                break;
            case Types.FLOAT:
                Float aFloat = Float.valueOf(pkStr);
                prepStmt.setFloat(idx, aFloat);
                break;
            case Types.REAL:        //not specified
            case Types.DOUBLE:
                Double aDouble = Double.valueOf(pkStr);
                prepStmt.setDouble(idx,aDouble);
                break;
            case Types.DECIMAL:     //not specified
            case Types.NUMERIC:
                BigDecimal numeric = BigDecimal.valueOf(Long.parseLong(pkStr));
                prepStmt.setBigDecimal(idx, numeric);
                break;
            case Types.CHAR:        //not specified
            case Types.VARCHAR:
                String str = String.valueOf(pkStr);
                prepStmt.setString(idx,str);
                break;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                Timestamp timestamp = Timestamp.valueOf(pkStr);
                prepStmt.setTimestamp(idx, timestamp);
                break;
            case Types.BLOB:
            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
            case Types.CLOB:
            case Types.LONGVARCHAR:
            case Types.NULL:
            case Types.DISTINCT:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.ARRAY:
            case Types.REF:
            case Types.STRUCT:
            default:
                logger.warn("Oracle未实现以"+fieldType+"类型字段作为主键的处理");
                break;
        }
        return prepStmt;
    }

    @Override
    public FieldValue setFieldValue(String appName,String sourceTableName,
                                           FieldValue fieldValue, ResultSet rs) throws SQLException, IOException {
        String fieldName = fieldValue.getFieldName();
        fieldValue = setPrivateFieldValue(appName,sourceTableName,fieldName,fieldValue,rs);
        return fieldValue;
    }

    private FieldValue setPrivateFieldValue(String appName,String sourceTableName,String fieldName,
                                            FieldValue fieldValue, ResultSet rs)  throws SQLException, IOException{

        String jdbcType = fieldValue.getJdbcType();
        int jdbcTypeInt = DBUtils.getJdbcType(jdbcType);
        switch (jdbcTypeInt) {
            case Types.BIT:
                fieldValue.setObj(rs.getBoolean(fieldName));
                break;
            case Types.TINYINT:
                fieldValue.setObj(rs.getByte(fieldName));
                break;
            case Types.SMALLINT:
                fieldValue.setObj(rs.getShort(fieldName));
                break;
            case Types.INTEGER:
                fieldValue.setObj(rs.getInt(fieldName));
                break;
            case Types.BIGINT:
                fieldValue.setObj(rs.getLong(fieldName));
                break;
            case Types.FLOAT:
                fieldValue.setObj(rs.getFloat(fieldName));
                break;
            case Types.REAL:        //not specified
                fieldValue.setObj(rs.getFloat(fieldName));
                break;
            case Types.DOUBLE:
                fieldValue.setObj(rs.getDouble(fieldName));
                break;
            case Types.DECIMAL:     //not specified
            case Types.NUMERIC:
                fieldValue.setObj(rs.getBigDecimal(fieldName));
                break;
            case Types.CHAR:        //not specified
            case Types.VARCHAR:
                fieldValue.setObj(rs.getString(fieldName));
                break;
            case Types.DATE:
                Date date = rs.getTimestamp(fieldName);
                fieldValue.setObj(date);
                break;
            case Types.TIME:
                fieldValue.setObj(rs.getTime(fieldName));
                break;
            case Types.TIMESTAMP:
                fieldValue.setObj(rs.getTimestamp(fieldName));
                break;
            case Types.BLOB:
            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
                Blob blob = rs.getBlob(fieldName);
                InputStream ins = blob.getBinaryStream();
                byte[] data = IOUtils.toByteArray(ins);
                ins.close();
                fieldValue.setObj(data);
                break;
            case Types.CLOB:
            case Types.LONGVARCHAR:
                Clob clob = rs.getClob(fieldName);
                Reader reader = clob.getCharacterStream();
                if(reader != null) {
                    StringBuffer sb = new StringBuffer();
                    char[] charbuff = new char[4096];
                    for(int i = reader.read(charbuff);i>0;i=reader.read(charbuff)) {
                        sb.append(charbuff,0,i);
                    }
                    fieldValue.setObj(sb.toString());
                } else {
                    logger.warn("应用" + appName + "读取表"+sourceTableName+ "的字段"+fieldName+"为空");
                    fieldValue.setObj(null);
                }
                break;
            case Types.NULL:
            case Types.DISTINCT:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
                fieldValue.setObj(rs.getObject(fieldName));
                break;
            case Types.ARRAY:
                fieldValue.setObj(rs.getArray(fieldName));
                break;
            case Types.REF:
                fieldValue.setObj(rs.getRef(fieldName));
                break;
            case Types.STRUCT:
                Struct struct = (Struct) rs.getObject(fieldName);
                if(struct.getSQLTypeName().indexOf("MDSYS")>-1
                        ||struct.getSQLTypeName().indexOf("SDO_GEOMETRY")>-1) {
                    int gtype = ((BigDecimal) struct.getAttributes()[0]).intValue();
                    STRUCT st = STRUCT.toSTRUCT(struct, (OracleConnection)null);
                    JGeometry jGeometry = JGeometry.load(st);
                    int srid = jGeometry.getSRID();
                    double[] xyz = jGeometry.getLabelPointXYZ();
                    double x = xyz[0];
                    if(Double.isNaN(x)){
                    }
                    int[] elemInfos = jGeometry.getElemInfo();
                    double[] ordinates = jGeometry.getOrdinatesArray();
                    String elem = null;
                    String ordi = null;
                    if(elemInfos != null && elemInfos.length>0) {
                        elem = String.valueOf(elemInfos[0]);
                        for (int i = 1; i < elemInfos.length; i ++) {
                            elem += "|" + elemInfos[i];
                        }
                    }
                    if(ordinates != null && ordinates.length>0) {
                        ordi = String.valueOf(ordinates[0]);
                        for (int i = 1; i < ordinates.length; i ++) {
                            ordi += "|" + ordinates[i];
                        }
                    }
                    String basicValue = "{g:"+gtype+",s:"+srid+
                            ",x:'"+xyz[0]+"',y:'"+xyz[1]+"',z:'"+xyz[2]+
                            "',e:'"+elem+"',o:'"+ordi+"'}";
                    fieldValue.setObj(basicValue);
                }
                break;
            default:
                logger.warn("Oracle未实现的字段类型"+fieldValue.getDbType());
                break;
        }
        return fieldValue;
    }

    @Override
    public FieldValue setFieldValueTwoWay(String appName, String sourceTableName,
                                          FieldValue fieldValue, ResultSet rs, boolean isSourceToTarget) throws SQLException, IOException {
        String fieldName;
        if(isSourceToTarget){
            fieldName = fieldValue.getFieldName();
        } else {
            fieldName = fieldValue.getDestField();
        }
        fieldValue = setPrivateFieldValue(appName,sourceTableName,fieldName,fieldValue,rs);
        return fieldValue;
    }

    @Override
    public String createSql_SelectFromSource(String appName, String Sql_View_Source, String sourceTableName, String flag, int syncFlagValue) {
        String Sql_SelectFromSource;  //源表查询
        if(flag!=null){
            Sql_SelectFromSource = "SELECT * FROM ( SELECT "+Sql_View_Source+", ROWNUM RN " +"FROM ("+
                    "SELECT " + Sql_View_Source + " FROM " + sourceTableName+" WHERE "+flag+"="+syncFlagValue+")" +
                    " WHERE ROWNUM <= ? ) WHERE RN >= ?";
        } else {
            Sql_SelectFromSource = "SELECT * FROM ( SELECT "+Sql_View_Source+", ROWNUM RN " +"FROM ("+
                    "SELECT " + Sql_View_Source + " FROM " + sourceTableName+")" +
                    " WHERE ROWNUM <= ? ) WHERE RN >= ?";

        }
        return Sql_SelectFromSource;
    }

    @Override
    public String createSql_SelectDate(String appName, String dbType, String sourceTableName, String flag, boolean isDesc) {
        if(isDesc){
            return "SELECT " + flag + " FROM (SELECT " + flag + ",ROWNUM rn FROM " + sourceTableName + " ORDER BY " + flag + " DESC) WHERE rn = 1";
        } else {
            return "SELECT " + flag + " FROM (SELECT " + flag + ",ROWNUM rn FROM " + sourceTableName + " ORDER BY " + flag + " ASC) WHERE rn = 1";
        }
    }

    @Override
    public String createSql_SelectFromSourceDate(String appName, String dbType, String Sql_View_Source, String sourceTableName, String flag) {
        return "SELECT * FROM ( SELECT " + Sql_View_Source + ", ROWNUM RN " + "FROM ("
                + "SELECT " + Sql_View_Source + " FROM " + sourceTableName + " WHERE " + flag
                + " between to_timestamp(?, 'yyyy-mm-dd hh24:mi:ss.ff') and to_timestamp(?, 'yyyy-mm-dd hh24:mi:ss.ff')"
                + ")  WHERE ROWNUM <= ? ) WHERE RN >= ?";
    }
}
