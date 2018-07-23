package com.wfu.kafkahbasetest.thread;

import com.wfu.kafkahbasetest.bean.Receiver;
import com.wfu.kafkahbasetest.util.HBaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.*;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

public class HandleData {

    public static boolean dohandleData( ) throws IOException {
//        JSONObject data = JSONObject.parseObject(message);
        String tableName = "test";
//        String address = null;
        String rowKey = UUID.randomUUID().toString().substring(1,5);

        Connection conn = HBaseUtils.getConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(Bytes.toBytes(HBaseUtils.DEFAULT_FAMILY), Bytes.toBytes("test"), Bytes.toBytes(Math.random()*10));

        try {
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            HBaseUtils.returnSource(conn, table);
        }
        return true;
    }
}
