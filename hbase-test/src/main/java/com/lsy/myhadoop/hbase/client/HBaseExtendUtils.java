package com.lsy.myhadoop.hbase.client;


import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class HBaseExtendUtils {
    private static Logger LOG = LoggerFactory.getLogger(HBaseExtendUtils.class);

    //判断表是否存在
    public static boolean tableExists(HBaseAdmin admin, String tablename) throws IOException {
        LOG.debug("enter HBaseExtendUtils.tableExists()");
        if (admin.tableExists(tablename)) {
            return true;
        } else {
            return false;
        }
    }

    //创建表
    public static void create(HBaseAdmin admin, String tablename, String columnFamily) throws Exception {
        LOG.debug("enter HBaseExtendUtils.create()");
        if (admin.tableExists(tablename)) {
            LOG.debug("Table:" + tablename + " already exists!");
            LOG.debug("Create table aborted!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tablename);
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
            LOG.debug("Table:" + tablename + " created successfully!");
        }
    }

    /**
     * 创建表
     *
     * @tableName 表名
     * @family 列族列表
     * @minVersions
     * @maxVersions
     */
    public static void createTable(HBaseAdmin admin, String tableName, String[] columnFamily, int minVersions, int maxVersions)
            throws Exception {
        LOG.debug("enter HBaseExtendUtils.createTable()");
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (int i = 0; i < columnFamily.length; i++) {
            desc.addFamily(new HColumnDescriptor(columnFamily[i]).setMinVersions(minVersions).setMaxVersions(maxVersions));
        }
        if (admin.tableExists(tableName)) {
            LOG.debug("table Exists!");
            LOG.debug("exit createTable");
            System.exit(0);
        } else {
            admin.createTable(desc);
            LOG.debug("create table Success!");
        }
    }

    /**
     * 删除表
     */
    public static void deleteTable(HBaseAdmin admin, String tablename) throws IOException {
        LOG.debug("enter HBaseExtendUtils.deleteTable()");
        if (admin.tableExists(tablename)) {
            try {
                admin.disableTable(tablename);
                admin.deleteTable(tablename);
                LOG.debug("Table:" + tablename + " deleted!");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        } else {
            LOG.debug("Table:" + tablename + " doesn't exist!");
        }
    }


    /**
     * 插入数据
     */
    public static void put(HConnection conn, String tablename, String rowKey, String columnFamily, String columnName, String data) throws Exception {
        LOG.debug("enter HBaseExtendUtils.put()");
        HTableInterface table = conn.getTable(tablename);
        LOG.debug("HBaseClient.put() method, already get table");
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(data));
        table.put(put);
        LOG.debug("put '" + rowKey + "','" + columnFamily + ":" + columnName + "','" + data + "'");
        table.close();
    }

    /**
     * 追加数据
     */
    public static void append(HConnection conn, String tablename, String rowKey, String columnFamily, String columnName, String data) throws Exception {
        LOG.debug("enter HBaseExtendUtils.append()");
        HTableInterface table = conn.getTable(tablename);
        Append append = new Append(Bytes.toBytes(rowKey));
        append.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(data));
        table.append(append);
        LOG.debug("append '" + rowKey + "','" + columnFamily + ":" + columnName + "','" + data + "'");
        table.close();
    }

    /**
     * 删除指定行的指定列
     *
     * @tableName 表名
     * @rowKey rowKey
     * @columnFamily 列族名
     * @columnName 列名
     */
    public static void deleteColumn(HConnection conn, String tableName, String rowKey,
                                    String columnFamily, String columnName) throws IOException {
        LOG.debug("enter HBaseExtendUtils.deleteColumn()");
        HTableInterface table = conn.getTable(Bytes.toBytes(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.deleteColumns(Bytes.toBytes(columnFamily),
                Bytes.toBytes(columnName));
        table.delete(deleteColumn);
        LOG.debug(columnFamily + ":" + columnName + "is deleted!");
        table.close();
    }

    /**
     * 更新表中的某一列
     *
     * @tableName 表名
     * @rowKey rowKey
     * @columnFamily 列族名
     * @columnName 列名
     * @value 更新后的值
     */
    public static void updateColumn(HConnection conn, String tableName, String rowKey,
                                    String columnFamily, String columnName, String data)
            throws IOException {
        LOG.debug("enter HBaseExtendUtils.updateColumn()");
        HTableInterface table = conn.getTable(Bytes.toBytes(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName),
                Bytes.toBytes(data));
        table.put(put);
        LOG.debug("update column Success");
        table.close();
    }

    /**
     * getMaxVersion
     */
    public static Result getMaxVersion(HConnection conn, String tablename, String rowKey) throws IOException {
        LOG.debug("enter HBaseExtendUtils.getMaxVersion()");
        HTableInterface table = conn.getTable(tablename);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.setMaxVersions();
        Result result = table.get(get);
        table.close();
        LOG.debug("Get: " + result);
        return result;
    }


    /**
     * 查询表中的某行(某一列)
     *
     * @tableName 表名
     * @rowKey rowKey
     * @columnFamily 列族明
     * @columnName 列名
     */
    public static Result getResult(HConnection conn, String tableName, String rowKey) throws IOException {
        LOG.debug("enter HBaseExtendUtils.getResultByColumn()");
        HTableInterface table = conn.getTable(Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        table.close();
        LOG.debug("Get Column: " + result);
        return result;
    }
    public static Result getResult(HConnection conn, String tableName, String rowKey,
                                   String columnFamily, String columnName) throws IOException {
        LOG.debug("enter HBaseExtendUtils.getResultByColumn()");
        HTableInterface table = conn.getTable(Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
        Result result = table.get(get);
        table.close();
        LOG.debug("Get Column: " + result);
        return result;
    }

    /**
     * 查询表中的多行
     * @param tableName
     * @param rowKeys
     * @return
     * @throws IOException
     */
    public static Result[] multiGetResult(HConnection conn, String tableName, Iterable<String> rowKeys) throws IOException {
        LOG.debug("enter HBaseExtendUtils.getResultByColumn()");
        HTableInterface table = conn.getTable(Bytes.toBytes(tableName));
        List<Get> queryRowList = new ArrayList<Get>();
        for(String rowkey : rowKeys){
            queryRowList.add(new Get(Bytes.toBytes(rowkey)));
        }
        Result[] results = table.get(queryRowList);
        table.close();
        LOG.debug("Get results: " + results);
        LOG.info("Get size:"+results[0].getValue("T".getBytes(),"c".getBytes()).length);
        return results;
    }
    public static Result[] multiGetResult(HConnection conn, String tableName, Iterable<String> rowKeys,
                                          String columnFamily, String columnName) throws IOException {
        LOG.debug("enter HBaseExtendUtils.getResultByColumn()");
        HTableInterface table = conn.getTable(Bytes.toBytes(tableName));
        List<Get> queryRowList = new ArrayList<Get>();
        for(String rowKey : rowKeys){
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
            queryRowList.add(get);
        }
        Result[] results = table.get(queryRowList);
        table.close();
        LOG.debug("Get results: " + results);
        return results;
    }


    /**
     * 遍历查询hbase表,过滤指定列族,返回ResultScanner
     *
     * @tableName 表名
     * @familyName 列族名称
     */
    public static ResultScanner scanResultByFamily(HConnection conn, String tableName, String columnFamily,
                                                   String startKey, String endKey) throws IOException {
        LOG.debug("enter HBaseExtendUtils.getResultScanByFamily()");
        HTableInterface table = conn.getTable(Bytes.toBytes(tableName));
        Scan scan = new Scan(Bytes.toBytes(startKey), Bytes.toBytes(endKey));
        Filter filter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(columnFamily)));
        scan.setFilter(filter);
        ResultScanner rs = table.getScanner(scan);
        table.close();
        return rs;
    }

    /**
     * row前缀匹配查询,返回ResultScanner
     *
     * @tableName 表名
     * @rowPrifix rowKey前缀
     */
    public ResultScanner scanResultByPrefixFilter(HConnection conn, String tablename, String rowPrifix) {
        LOG.debug("enter HBaseExtendUtils.scaneByPrefixFilter()");
        try {
            HTableInterface table = conn.getTable(Bytes.toBytes(tablename));
            Scan s = new Scan();
            s.setFilter(new PrefixFilter(rowPrifix.getBytes()));
            ResultScanner rs = table.getScanner(s);
            table.close();
            return rs;
        } catch (IOException e) {
            LOG.debug(e.toString());
            return null;
        }
    }

    /**
     * 前缀匹配查询,并通过 列族  列名 过滤
     * <p>
     * 返回ResultScanner
     *
     * @tableName 表名
     * @rowPrifix rowKey前缀
     */

    public ResultScanner scanResultByPrefixFilter(HConnection conn, String tablename, String rowPrifix,
                                                  String columnFamily[], String columnName[], String values[]) {
        LOG.debug("enter HBaseExtendUtils.scaneByPrefixFilter()");
        try {
            HTableInterface table = conn.getTable(Bytes.toBytes(tablename));
            Scan scan = new Scan();
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

            for (int i = 0; i < columnFamily.length; i++) {
                Filter filter = new SingleColumnValueFilter(
                        Bytes.toBytes(columnFamily[i]),
                        Bytes.toBytes(columnName[i]),
                        CompareFilter.CompareOp.EQUAL,
                        Bytes.toBytes(values[i])
                );
                filterList.addFilter(filter);
            }
            filterList.addFilter(new PrefixFilter(rowPrifix.getBytes()));
            scan.setFilter(filterList);
            ResultScanner rs = table.getScanner(scan);
            table.close();
            return rs;
        } catch (IOException e) {
            LOG.debug(e.toString());
            return null;
        }
    }

    /**
     * ResultScanner
     */
//    for (Result r : rs) {
//        for (KeyValue kv : r.list()) {
//            System.out.println("row:" + Bytes.toString(kv.getRow()));
//            System.out.println("family:"
//                    + Bytes.toString(kv.getFamily()));
//            System.out.println("qualifier:"
//                    + Bytes.toString(kv.getQualifier()));
//            System.out
//                    .println("value:" + Bytes.toString(kv.getValue()));
//            System.out.println("timestamp:" + kv.getTimestamp());
//            System.out
//                    .println("-------------------------------------------");
//        }
//    }
}





