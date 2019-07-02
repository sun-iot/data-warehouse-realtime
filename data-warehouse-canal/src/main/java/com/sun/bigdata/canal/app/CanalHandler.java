package com.sun.bigdata.canal.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.sun.bigdata.canal.util.MyKafkaSender;
import com.sun.bigdata.warehouse.common.constant.GmallConstants;

import java.util.List;


public class CanalHandler {

    private List<CanalEntry.RowData> rowDatasList;
    String tableName;
    CanalEntry.EventType eventType;

    /**
     * @param rowDatasList
     * @param tableName
     * @param eventType
     */
    public CanalHandler(List<CanalEntry.RowData> rowDatasList, String tableName, CanalEntry.EventType eventType) {
        this.rowDatasList = rowDatasList;
        this.tableName = tableName;
        this.eventType = eventType;
    }

    /**
     *
     */
    public void handle() {
        if (eventType.equals(CanalEntry.EventType.INSERT) && tableName.equals("order_info")) {
            sendRowList2Kafka(GmallConstants.KAFKA_TOPIC_ORDER);
        } else if ((eventType.equals(CanalEntry.EventType.INSERT) || eventType.equals(CanalEntry.EventType.UPDATE)) && tableName.equals("user_info")) {
            sendRowList2Kafka(GmallConstants.KAFKA_TOPIC_USER);
        }

    }

    /**
     * @param kafkaTopic
     */
    private void sendRowList2Kafka(String kafkaTopic) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {

                System.out.println(column.getName() + "-->" + column.getValue());
                jsonObject.put(column.getName(), column.getValue());
            }

            MyKafkaSender.send(kafkaTopic, jsonObject.toJSONString());
        }

    }

}
