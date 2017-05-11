package org.gislab.hbase.showcase;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.gislab.hbase.util.GeoToolsHelper;
import org.gislab.hbase.util.HBaseHelper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ladyfish on 2017/5/10.
 */
public class Example {

    public static void processData(){
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("core-site.xml");
        System.out.println(conf.get("hadoop.home.dir"));
        System.out.println(conf.get("fs.defaultFS"));
        System.out.println(conf.get("hbase.master.info.bindAddress"));
        System.out.println(conf.get("hbase.master.info.port"));
        GeoToolsHelper geoToolsHelper = GeoToolsHelper.getGeoToolsHelper();
        String[] fams=new String[]{"CF_PROP","CF_GEO","CF_STAT"};
        try(HBaseHelper help=HBaseHelper.getHBaseHelper(conf);BufferedReader bReader=new BufferedReader(new FileReader("E:\\2017\\AREAWATER.csv\\AREAWATER.csv"))){
            help.dropTable("AREAWATER");
            help.createTable("AREAWATER",fams, Bytes.toBytes("H20301101668801592"),Bytes.toBytes("H3020110340951146"),5);
            String line=null;
             long count=0;
            List<Put> putlst=new ArrayList<>();
            while((line=bReader.readLine())!=null){
                String[] arr=line.replace("\"","").split("\t");
                Geometry geometry=geoToolsHelper.createGeometryFromWKT(arr[0]);
                String rowKey=arr[4]+arr[2];
                putlst.add(help.buildPut(rowKey,"CF_PROP",new String[]{"ANSICODE","HYDROID","FULLNAME","MTFCC"},new String[]{arr[1],arr[2],arr[3],arr[4]}));
                putlst.add(help.buildPut(rowKey,"CF_GEO",new String[]{"WKT"},new String[]{arr[0]}));
                putlst.add(help.buildPut(rowKey,"CF_STAT",new String[]{"ALAND","AWATER","INTPTLAT","INTPTLON"},new String[]{arr[5],arr[6],arr[7],arr[8]}));
                count++;
                if(putlst.size()>5000){
                    help.put("AREAWATER",putlst);
                    putlst.clear();
                    System.out.println(count);
                }
            }
            if(putlst.size()>0){
                help.put("AREAWATER",putlst);
            }
            System.out.println(count);
        }catch (IOException ex){
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        query1();
    }
    public static void query(){
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("core-site.xml");
        try(HBaseHelper help=HBaseHelper.getHBaseHelper(conf)){
            long stimestamp=System.currentTimeMillis();
            ResultScanner rscan=help.getScanner("AREAWATER","CF_PROP","HYDROID","110450969729");
            Result rs=rscan.next();
            if(rs!=null){
                System.out.println( rs.toString());
            }
            System.out.println(System.currentTimeMillis()-stimestamp);
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void query1(){
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("core-site.xml");
        try(HBaseHelper help=HBaseHelper.getHBaseHelper(conf)){
            long stimestamp=System.currentTimeMillis();
            System.out.println(help.get("AREAWATER","H2030110515108523"));
            System.out.println(System.currentTimeMillis()-stimestamp);
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
}
