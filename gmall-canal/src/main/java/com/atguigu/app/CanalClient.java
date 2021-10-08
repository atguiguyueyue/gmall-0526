package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1.获取连接对象
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while (true) {
            //2.获取连接
            connector.connect();

            //3.指定订阅对象
            connector.subscribe("gmall.*");

            //4.获取数据
            Message message = connector.get(100);

            //5.获取每一条sql对应数据
            List<CanalEntry.Entry> entries = message.getEntries();
            if (entries.size() > 0) {
                //证明有数据存在
                for (CanalEntry.Entry entry : entries) {
                    //TODO 6.获取表名
                    String tableName = entry.getHeader().getTableName();

                    //7.获取entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();

                    //8.判断entry类型获取数据
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        //9.获取序列化的数据
                        ByteString storeValue = entry.getStoreValue();

                        //10.反序列化数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        //TODO 11.获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //TODO 12.获取数据集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        //将数据发送至kafka
                        handle(tableName, eventType, rowDatasList);
                    }
                }

            } else {
                System.out.println("没有数据休息一会。。。。");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    private static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //1.根据表名判断获取的数据来源，根据事件类型判断获取新增或者变化的数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER);
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_USER);
        }
    }

    private static void saveToKafka(List<CanalEntry.RowData> rowDatasList, String kafkaTopicOrder) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }
            System.out.println(jsonObject.toString());
            //模拟网络震荡/网络延迟效果
            try {
                Thread.sleep(new Random().nextInt(5)*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //将数据发送至Kafka
            MyKafkaSender.send(kafkaTopicOrder, jsonObject.toString());
        }
    }

}
