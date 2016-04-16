package com.hzih.db.utils;

import com.hzih.db.entity.FieldValue;
import com.hzih.db.entity.Sequence;
import com.hzih.db.entity.Table;
import com.hzih.db.entity.Type;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 15-11-9.
 */
public class Configuration {

    public static Type getType(String appName,String configXml) throws Exception {
        SAXReader saxReader = new SAXReader();
        Document doc = saxReader.read(configXml);
        Element typeNode = (Element) doc.selectSingleNode("/configuration/system/ichange/types/type[@value='" + appName + "']");

        String apptype = typeNode.attribute("apptype").getValue();
        String appDesc = typeNode.attribute("desc").getValue();
        String sourcedb = typeNode.selectSingleNode("sourcedb").getText();
        String sourcetemptable= typeNode.selectSingleNode("sourcetemptable")==null
                ?null:typeNode.selectSingleNode("sourcetemptable").getText();
        String targetdb = typeNode.selectSingleNode("targetdb").getText();
        String targettemptable = typeNode.selectSingleNode("targettemptable")==null
                ?null:typeNode.selectSingleNode("targettemptable").getText();
        String maxrecords = typeNode.selectSingleNode("maxrecords").getText();
        String interval = typeNode.selectSingleNode("interval").getText();
        String isactive = typeNode.selectSingleNode("isactive").getText();
        Type type = new Type();
        type.setAppName(appName);
        type.setAppType(apptype);
        type.setAppDesc(appDesc);
        type.setSourceDB(sourcedb);
        type.setSourceTempTable(sourcetemptable);
        type.setTargetDB(targetdb);
        type.setTargetTempTable(targettemptable);
        type.setMaxRecords(Integer.parseInt(maxrecords));
        type.setInterval(Integer.parseInt(interval));
        type.setActive(Boolean.parseBoolean(isactive));

        List<Element> list = typeNode.selectNodes("tables/table");
        List<Table> tableList = new ArrayList<Table>();
        for(Element tableNode : list) {
            Table table = new Table();
            table.setSourceTableName(tableNode.attribute("value").getValue());
            table.setTargetTableName(tableNode.attribute("dest").getValue());
            Node flagNode = tableNode.selectSingleNode("flag");
            if(flagNode!=null) {
                Element fNode = (Element) tableNode.selectSingleNode("flag");
                table.setFlagBefore(Integer.parseInt(fNode.attribute("before").getValue()));
                table.setFlagAfter(Integer.parseInt(fNode.attribute("after").getValue()));
                table.setFlagName(fNode.getText());
            }
            table.setTableSeqNumber(Integer.parseInt(tableNode.selectSingleNode("seqnumber").getText()));
            table.setTargetOnlyInsert(Boolean.parseBoolean(tableNode.selectSingleNode("onlyinsert") == null
                    ? "false" : tableNode.selectSingleNode("onlyinsert").getText()));
            table.setTargetDeleteAble(Boolean.parseBoolean(tableNode.selectSingleNode("deleteenable") == null
                    ? "false" : tableNode.selectSingleNode("deleteenable").getText()));
            table.setSourceDeleteAble(Boolean.parseBoolean(tableNode.selectSingleNode("deletesource")==null
                    ?"false":tableNode.selectSingleNode("deletesource").getText()));
            table.setOnlyOnce(Boolean.parseBoolean(tableNode.selectSingleNode("onlyonce") == null
                    ? "false" : tableNode.selectSingleNode("onlyonce").getText()));

            List<Element> fieldList = tableNode.selectNodes("fields/field");
            List<FieldValue> fieldValueList = new ArrayList<FieldValue>();
            for (Element fieldNode : fieldList){
                FieldValue fieldValue = new FieldValue();
                fieldValue.setFieldName(fieldNode.attribute("value").getValue());
                fieldValue.setDestField(fieldNode.attribute("dest").getValue());
                fieldValue.setPk(fieldNode.selectSingleNode("IS_PK").getText());
                fieldValue.setColumnSize(fieldNode.selectSingleNode("COLUMN_SIZE").getText());
                fieldValue.setDbType(fieldNode.selectSingleNode("DB_TYPE").getText());
                fieldValue.setJdbcType(fieldNode.selectSingleNode("JDBC_TYPE").getText());
                fieldValue.setNull(fieldNode.selectSingleNode("IS_NULL").getText());
                fieldValueList.add(fieldValue);
            }
            table.setFieldValueList(fieldValueList);
            tableList.add(table);
        }
        type.setTableList(tableList);

        List<Element> sequenceList = typeNode.selectNodes("sequences/sequence");
        List<Sequence> sequences = new ArrayList<Sequence>();
        for(Element sequenceNode : sequenceList) {
            Sequence sequence = new Sequence();
            sequence.setSourceSequenceName(sequenceNode.attribute("value").getText());
            sequence.setTargetSequenceName(sequenceNode.attribute("dest").getText());
            sequences.add(sequence);
        }
        type.setSequenceList(sequences);


        return type;
    }
}
