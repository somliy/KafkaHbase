package com.wfu.kafkahbasetest.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Component;


import java.util.ArrayList;
import java.util.List;

public class HBaseUtils {
    /**
     * 与HBase数据库的连接对象
     */
    private static Connection connection;
    /**
     * 数据库元数据操作对象
     */
    private static Admin admin;
    /**
     * 取得一个数据库连接的配置参数对象
     */
    private static Configuration conf = HBaseConfiguration.create();
    /**
     * 默认列簇
     */
    public static String DEFAULT_FAMILY = "cf";

    static {
        // 设置连接参数：HBase数据库所在的主机IP 大数据
        conf.set("hbase.zookeeper.quorum", "192.168.1.119");
        // 设置连接参数：HBase数据库使用的端口
        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        // 设置连接参数：Master链接地址
        conf.set("hbase.master", "192.168.1.119:16010");

        System.setProperty("hadoop.home.dir","D:\\hadoop");
        // 设置请求超时时间
        conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 1200000);
    }
    public static Connection getConn(){
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (Exception e){
            returnSource(connection);
            e.printStackTrace();
        }
        return connection;
    }

    public static Admin getAdmin(){
        try {
            if(admin == null){
                admin = getConn().getAdmin();
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return admin;
    }

    /**
     * 释放占有的资源
     * @param table
     */
    public static void returnSource(Table table){
        returnSource(null, table);
    }

    /**
     * 释放占有的资源
     * @param conn
     */
    public static void returnSource(Connection conn){
        returnSource(conn, null);
    }
    /**
     * 释放占有的资源
     * @param conn
     * @param table
     */
    public static void returnSource(Connection conn, Table table){
        try {
            if(table != null){
                table.close();
            }
            if(conn != null){
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
