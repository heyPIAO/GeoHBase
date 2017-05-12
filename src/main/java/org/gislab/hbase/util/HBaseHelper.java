package org.gislab.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * HBase API 的二次封装
 * Created by ladyfish on 2017/5/10.
 */
public class HBaseHelper implements Closeable{

    private Configuration configuration = null;
    private Connection connection = null;           //重量级对象
    private Admin admin = null;

    protected HBaseHelper(Configuration conf) throws IOException {
        this.configuration=conf;
        connection=ConnectionFactory.createConnection(this.configuration);
        this.admin=connection.getAdmin();
    }

    /**
     * 获取HBaseHelper对象
     * @param conf
     * @return HBaseHelper
     * @throws IOException
     */
    public static HBaseHelper getHBaseHelper(Configuration conf) throws IOException {
        return new HBaseHelper(conf);
    }

    /**
     * 创建表
     * @param tableName 表名
     * @param families 列族列表
     * @throws IOException
     */
    public void createTable(String tableName,String... families) throws IOException{
        HTableDescriptor hTableDesc=createTableDescription(tableName,families);
        if(hTableDesc!=null){
            admin.createTable(hTableDesc);
        }
    }

    /**
     * 创建表，并设置表的regions数量
     * @param tableName 表描述
     * @param families 列族列表
     * @param startKey 键值起始值
     * @param endKey 键值终止值
     * @param numRegions 分区数量
     * @throws IOException
     */
    public void createTable(String tableName,String[] families,byte[] startKey,byte[] endKey,int numRegions) throws IOException {
        HTableDescriptor hTableDesc=createTableDescription(tableName,families);
        if(hTableDesc!=null){
            admin.createTable(hTableDesc,startKey,endKey,numRegions);
        }
    }
    /**
     * 创建表，并设置表的初始划分
     * @param tableName 表名
     * @param families 列族列表
     * @param splitKeys 拆分的key值列表
     * @throws IOException
     */
    public void createTable(String tableName,String[] families,String[] splitKeys) throws IOException{
        HTableDescriptor hTableDesc=createTableDescription(tableName,families);
        byte[][] splitKeysb=new byte[splitKeys.length][];
        for(int i=0;i<splitKeys.length;i++){
            splitKeysb[i]= Bytes.toBytes(splitKeys[i]);
        }
        if(hTableDesc!=null){
            admin.createTable(hTableDesc,splitKeysb);
        }
    }

    /**
     * 标志一张表不可用
     * @param table 表名
     * @throws IOException
     */
    public void disableTable(String table) throws IOException {
        disableTable(TableName.valueOf(table));
    }

    /**
     * 标志一张表不可用
     * @param table
     * @throws IOException
     */
    public void disableTable(TableName table) throws IOException {
        admin.disableTable(table);
    }

    /**
     * 删除一张表
     * @param table
     * @throws IOException
     */
    public void dropTable(String table) throws IOException {
        dropTable(TableName.valueOf(table));
    }

    /**
     * 删除一张表
     * @param table
     * @throws IOException
     */
    public void dropTable(TableName table) throws IOException {
        if(admin.tableExists(table)){
            if(admin.isTableEnabled(table)) admin.disableTable(table);
            admin.deleteTable(table);
        }
    }

    /**
     * 列出所有表
     * @return
     * @throws IOException
     */
    public HTableDescriptor[] listTables() throws IOException {
        HTableDescriptor[] hTableDescriptors = admin.listTables();
        return hTableDescriptors;
    }

    /**
     * 向表中插入某个qualify的值
     * @param table
     * @param row
     * @param fam
     * @param qual
     * @param value
     * @throws IOException
     */
    public void put(String table,String row,String fam,String qual,String value) throws IOException {
        put(TableName.valueOf(table),row,fam,qual,value);
    }

    /**
     * 向表中插入某个qualify的值
     * @param table
     * @param row
     * @param fam
     * @param qual
     * @param value
     * @throws IOException
     */
    public void put(TableName table,String row,String fam,String qual,String value) throws IOException {
        Table tb1 = connection.getTable(table);
        Put put=new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(fam),Bytes.toBytes(qual),Bytes.toBytes(value));
        tb1.put(put);
        tb1.close();
    }

    /**
     * 导入多条数据
     * @param table
     * @param row
     * @param fam
     * @param quals
     * @param timeStamps
     * @param values
     * @throws IOException
     */
    public void put(String table,String row,String fam,String[] quals,long[] timeStamps,String[] values) throws IOException {
        put(TableName.valueOf(table),row,fam,quals,timeStamps,values);
    }

    /**
     * 导入多条数据
     * @param table
     * @param row
     * @param fam
     * @param quals
     * @param timeStamps
     * @param values
     * @throws IOException
     */
    public void put(TableName table,String row,String fam,String[] quals,long[] timeStamps,String[] values) throws IOException {
        if(quals.length!=values.length||quals.length!=timeStamps.length){
            return;
        }
        Table tb1 = connection.getTable(table);
        List<Put> puts=new ArrayList<>();
        Put put=new Put(Bytes.toBytes(row));
        int i=0;
        for(String qual :quals){
            put.addColumn(Bytes.toBytes(fam),Bytes.toBytes(qual),timeStamps[i],Bytes.toBytes(values[i]));
            i++;
        }
        puts.add(put);
        tb1.put(puts);
        tb1.close();
    }

    /**
     * 批量入库
     * @param table
     * @param puts
     */
    public void put(String table,List<Put> puts){
        try {
            Table tb1 = connection.getTable(TableName.valueOf(table));
            tb1.put(puts);
            tb1.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建一个Put对象
     * @param row rowkey
     * @param fam 列簇名称
     * @param quals 列名称
     * @param values 值
     * @return
     */
    public Put buildPut(String row,String fam,String[] quals,String[] values){
        if(quals.length!=values.length){
            return null;
        }
        Put put=new Put(Bytes.toBytes(row));
        int i=0;
        for(String qual :quals){
            put.addColumn(Bytes.toBytes(fam),Bytes.toBytes(qual),Bytes.toBytes(values[i]));
            i++;
        }
        return put;
    }

    /**
     * 创建表的描述
     * @param tableName 表名
     * @param families 列族名
     * @return 表的描述对象
     * @throws IOException
     */
    private HTableDescriptor createTableDescription(String tableName,String... families) throws IOException {
        TableName tName=TableName.valueOf(tableName);
        HTableDescriptor hTableDesc = null;
        if(admin.tableExists(tName)){
            throw new TableExistsException();
        }else{
            hTableDesc =new HTableDescriptor(tName);
            for(String family : families){
                HColumnDescriptor hColumnDesc = new HColumnDescriptor(family);
                hTableDesc.addFamily(hColumnDesc);
            }
        }
        return hTableDesc;
    }

    /**
     * 根据某一列的值获取记录 EQUAL
     * @param table
     * @param fam
     * @param col
     * @param value
     * @return
     */
    public ResultScanner getScanner(String table,String fam,String col,String value){
        try {
            Table tb1 = connection.getTable(TableName.valueOf(table));
            Scan scan=new Scan();
            scan.setCaching(2000);
            scan.setMaxVersions();
            SingleColumnValueFilter filter=new SingleColumnValueFilter(Bytes.toBytes(fam),Bytes.toBytes(col), CompareFilter.CompareOp.EQUAL,Bytes.toBytes(value));
            scan.setFilter(filter);
            return tb1.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 根据表明与键值获取记录
     * @param table
     * @param rowkey
     * @return
     */
    public Result get(String table,String rowkey){
        try {
            Table tb1 = connection.getTable(TableName.valueOf(table));
            Get get=new Get(Bytes.toBytes(rowkey));
            Result result = tb1.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 关闭与HBase的所有连接
     * @throws IOException
     */
    public void close() throws IOException {
        if(admin!=null){
            admin.close();
        }
        if(connection!=null&&!connection.isClosed()){
            connection.close();
        }
    }

}
