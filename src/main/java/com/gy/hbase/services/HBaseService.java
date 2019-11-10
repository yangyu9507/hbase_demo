package com.gy.hbase.services;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

/**
 * created by yangyu on 2019-11-08
 */
@Service
@Slf4j
public class HBaseService {

    @Autowired
    private Connection connection;

    /**
     * @param tableName
     *            创建一个表 tableName 指定的表名　 seriesStr
     * @param seriesStr
     *            以字符串的形式指定表的列族，每个列族以逗号的形式隔开,(例如：＂f1,f2＂两个列族，分别为f1和f2)
     **/
    public boolean createTable(String tableName, String seriesStr) throws IOException{
        // 判断是否创建成功！初始值为false
        boolean isSuccess = false;
        TableName table = TableName.valueOf(tableName);
        Admin admin = connection.getAdmin();
        try {

            if (!admin.tableExists(table)) {
                log.info("INFO:Hbase::  " + tableName + "原数据库中表不存在！开始创建...");
                HTableDescriptor descriptor = new HTableDescriptor(table);
                String[] series = seriesStr.split(",");
                for (String s : series) {
                    descriptor.addFamily(new HColumnDescriptor(s.getBytes()));
                }
                admin.createTable(descriptor);
                log.info("INFO:Hbase::  "+tableName + "新的" + tableName + "表创建成功！");
                isSuccess = true;
            } else {
                log.info("INFO:Hbase::  该表已经存在，不需要在创建！");
            }
        } catch (Exception e) {
            log.error("create table failed: ",e);
        } finally {
            IOUtils.closeQuietly(admin);
        }
        return isSuccess;
    }

    /**
     * 删除指定表名的表
     * @param tableName 　表名
     * @throws IOException
     * */
    public boolean dropTable(String tableName) throws IOException {
        boolean isSuccess = false;// 判断是否创建成功！初始值为false
        TableName table = TableName.valueOf(tableName);
        Admin admin = connection.getAdmin();
        try {
            if (admin.tableExists(table)) {
                admin.disableTable(table);
                admin.deleteTable(table);
                isSuccess = true;
            }
        } finally {
            IOUtils.closeQuietly(admin);
        }
        return isSuccess;
    }


    /**
     * 向指定表中插入数据
     *
     * @param tableName
     *            要插入数据的表名
     * @param rowkey
     *            指定要插入数据的表的行键
     * @param family
     *            指定要插入数据的表的列族family
     * @param qualifier
     *            要插入数据的qualifier
     * @param value
     *            要插入数据的值value
     * */
    public void putDataH(String tableName, String rowkey, String family,
                                 String qualifier, Object value) throws IOException {
        TableName tN = TableName.valueOf(tableName);
        Admin admin = connection.getAdmin();
        if (admin.tableExists(tN)) {
            try (
                    Table table = connection.getTable(TableName.valueOf(tableName.getBytes()))
                )
            {
                Put put = new Put(rowkey.getBytes());
                put.addColumn(family.getBytes(), qualifier.getBytes(),
                        value.toString().getBytes());
                table.put(put);
            } catch (Exception e) {
                log.error("插入数据的表不存在，请指定正确的tableName: ",e);
            }
        } else {
            log.error("插入数据的表不存在，请指定正确的tableName ! ");
        }
    }

    /**
     * 根据指定表获取指定行键rowkey和列族family的数据 并以字符串的形式返回查询到的结果
     *
     * @param tableName
     *            要获取表 tableName 的表名
     * @param rowKey
     *            指定要获取数据的行键
     * @param family
     *            指定要获取数据的列族元素
     * @param qualifier
     *            指定要获取数据的qualifier
     *
     * */
    public  String getValueBySeriesH(String tableName, String rowKey,
                                              String family,String qualifier) throws IllegalArgumentException, IOException {
        Table table = null;
        String resultStr = null;
        try {
            table = connection
                    .getTable(TableName.valueOf(tableName.getBytes()));
            Get get = new Get(Bytes.toBytes(rowKey));
            if( !get.isCheckExistenceOnly()){
                get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                Result res = table.get(get);
                byte[] result = res.getValue(Bytes.toBytes(family),
                        Bytes.toBytes(qualifier));
                resultStr = Bytes.toString(result);
            }else{
                resultStr = null;
            }
        } finally {
            IOUtils.closeQuietly(table);
        }
        return resultStr;
    }

    /**
     * 根据指定表获取指定行键rowKey和列族family的数据 并以Map集合的形式返回查询到的结果
     *
     * @param tableName
     *            要获取表 tableName 的表名
     * @param rowKey
     *            指定的行键rowKey
     * @param family
     *            指定列族family
     * */
    public  Map<String, String> getAllValueＨ(String tableName,
                                                      String rowKey, String family) throws IllegalArgumentException, IOException {
        Table table = null;
        Map<String, String> resultMap = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            if(get.isCheckExistenceOnly()){
                Result res = table.get(get);
                Map<byte[], byte[]> result = res.getFamilyMap(family.getBytes());
                Iterator<Map.Entry<byte[], byte[]>> it = result.entrySet().iterator();
                resultMap = new HashMap<String, String>();
                while (it.hasNext()) {
                    Map.Entry<byte[], byte[]> entry = it.next();
                    resultMap.put(Bytes.toString(entry.getKey()),
                            Bytes.toString(entry.getValue()));
                }
            }
        } finally {
            IOUtils.closeQuietly(table);
        }
        return resultMap;
    }


    /**
     * 根据指定表获取指定行键rowKey的所有数据 并以Map集合的形式返回查询到的结果
     * 每条数据之间用&&&将Qualifier和Value进行区分
     * @param tableName
     *            要获取表 tableName 的表名
     * @param rowkey
     *            指定的行键rowKey
     * */
    public ArrayList<String> getFromRowkeyValues(String tableName, String rowkey){
        Table table =null;
        ArrayList<String> Resultlist = new ArrayList<>();
        Get get =  new  Get(Bytes. toBytes ( rowkey ));
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Result  r = table.get(get);
            for  (Cell cell : r.rawCells()) {
                //每条数据之间用&&&将Qualifier和Value进行区分
                String reString = Bytes. toString (CellUtil. cloneQualifier (cell))+"&&&"+Bytes. toString (CellUtil. cloneValue (cell));
                Resultlist.add(reString);
            }
            table.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        return Resultlist;
    }

    /**
     * 根据表名获取所有的数据
     * */
    @SuppressWarnings("unused")
    private void getAllValues(String tableName){
        try {
            Table table= connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner resutScanner = table.getScanner(scan);
            for(Result result: resutScanner){
                log.info("scan:  " + result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public  void getTestDate(String tableName) throws IOException{
        Table table = null;
        table = connection.getTable(TableName.valueOf(tableName));
        int count = 0;
        Scan scan  = new Scan();
        scan.addFamily("f".getBytes());

        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator("112213.*"));
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner){
            log.info("result:{}", JSONObject.toJSONString(result));
            count++;
        }
        log.info("INFO:Hbase::  测试结束！共有　" + count + "条数据");
    }

}
