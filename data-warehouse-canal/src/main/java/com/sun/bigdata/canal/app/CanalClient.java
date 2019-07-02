package com.sun.bigdata.canal.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    public static void watch(String hostname, int port, String destination, String tables) {
        // 构造连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(hostname, port), destination, "", "");
        while (true) {
            canalConnector.connect();
            canalConnector.subscribe(tables);
            // message : 一次canal从日志中抓取的信息，一个message包含多个sql(event)
            Message message = canalConnector.get(100);
            int size = message.getEntries().size();
            if (size == 0) {
                System.out.println("没有数据，休息一会");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                //  entry: 相当于一个sql命令，一个sql可能会对多行记录造成影响
                for (CanalEntry.Entry entry : message.getEntries()) {

                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                        // rowchange ： entry经过反序列化得到的对象，包含了多行记录的变化值
                        CanalEntry.RowChange rowChange = null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                            // InvalidProtocolBuffer 一种序列化
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        // 一个rowchange里包含的数据变化集，其中每一个rowdata里面包含了一行的多个字段
                        // column: 一个RowData里包含了多个column，每个column包含了 name和 value
                        // 行集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        // 表名
                        String tableName = entry.getHeader().getTableName();
                        // 操作行为 insert update delete
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        CanalHandler canalHandler = new CanalHandler(rowDatasList, tableName, eventType);
                        canalHandler.handle();
                    }
                }
            }
        }
    }
}
