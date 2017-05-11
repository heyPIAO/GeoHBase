package org.gislab.hbase.util;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.util.AffineTransformation;
import com.vividsolutions.jts.geom.util.AffineTransformationBuilder;
import com.vividsolutions.jts.io.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

/**
 * GeoTools的二次封装,包括JTS
 * Created by ladyfish on 2017/5/10.
 */
public class GeoToolsHelper {
    private static GeoToolsHelper helper=new GeoToolsHelper();

    /**
     * 获取GeoTools的单例对象
     * @return
     */
    public static GeoToolsHelper getGeoToolsHelper(){
        return helper;
    }

    /**
     * 从WKT字符串构建Geometry
     * @param wkt
     * @return
     */
    public Geometry createGeometryFromWKT(String wkt){
        try{
            WKTReader wktReader=new WKTReader();
            Geometry geometry = wktReader.read(wkt);
            return geometry;
        }catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 从WKb字符串构建Geometry
     * @param wkb
     * @return
     */
    public Geometry createGeometryFromWKB(String wkb){
        return createGeometryFromWKB(Bytes.toBytes(wkb));
    }
    /**
     * 从WKb字节数组构建Geometry
     * @param wkbs
     * @return
     */
    public Geometry createGeometryFromWKB(byte[] wkbs){
        try{
            WKBReader wkbReader=new WKBReader();
            Geometry geometry = wkbReader.read(wkbs);
            return geometry;
        }catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 将geometry 转换为WKT字符串
     * @param geo
     * @return
     */
    public String getWKTFromGeometry(Geometry geo){
        WKTWriter writer=new WKTWriter();
        return writer.write(geo);
    }

    /**
     * 将geometry 转换为WKB字节数组
     * @param geo
     * @return
     */
    public byte[] getWKBFromGeometry(Geometry geo){
        WKBWriter writer=new WKBWriter();
        return writer.write(geo);
    }

    /**
     * 求两个Geometry之间的距离
     * @param geo1
     * @param geo2
     * @return
     */
    public double distance(Geometry geo1,Geometry geo2){
        return geo1.distance(geo2);
    }

    /**
     * 求两个图形之间相交的部分
     * @param geo1
     * @param geo2
     * @return
     */
    public Geometry intersect(Geometry geo1,Geometry geo2){
        return geo1.intersection(geo2);
    }

    /**
     * 求Geometry1是否包含Geometry2
     * @param geo1
     * @param geo2
     * @return
     */
    public boolean contains(Geometry geo1,Geometry geo2){
        return geo1.contains(geo2);
    }
    /**
     * 求Geometry1是否Touches Geometry2
     * @param geo1
     * @param geo2
     * @return
     */
    public boolean touches(Geometry geo1,Geometry geo2){
        return geo1.touches(geo2);
    }

    /**
     * 从WKT字符串构建坐标系
     * @param wkt
     * @return
     */
    public CoordinateReferenceSystem createReferenceFromWKT(String wkt){
        try {
            CoordinateReferenceSystem coordinateReferenceSystem = CRS.parseWKT(wkt);
            return coordinateReferenceSystem;
        } catch (FactoryException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 根据EPSG编号构建坐标系
     * @param epsgId
     * @return
     */
    public CoordinateReferenceSystem createReferenceFromEPSG(int epsgId){
        try {
            CoordinateReferenceSystem coordinateReferenceSystem = CRS.decode("EPSG:"+epsgId);
            return coordinateReferenceSystem;
        } catch (FactoryException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 通过三个变换前的点到三个变换后的点构建仿射变换
     * @param s1 源点1
     * @param s2 源点2
     * @param s3 源点3
     * @param t1 目标点1
     * @param t2 目标点2
     * @param t3 目标点3
     * @return
     */
    public AffineTransformation getAffineTransform(Coordinate s1,
                                                   Coordinate s2,
                                                   Coordinate s3,
                                                   Coordinate t1,
                                                   Coordinate t2,
                                                   Coordinate t3){
        AffineTransformationBuilder afb = new AffineTransformationBuilder(s1, s2, s3, t1, t2, t3);
        AffineTransformation atf = afb.getTransformation();
        return atf;
    }

    /**
     * 进行投影转换
     * @param geo
     * @param crs1
     * @param crs2
     * @return
     */
    public Geometry doReferenceTransform(Geometry geo, CoordinateReferenceSystem crs1,CoordinateReferenceSystem crs2) {
        try {
            MathTransform transform = CRS.findMathTransform(crs1, crs2);
            return JTS.transform(geo,transform);
        } catch (TransformException e) {
            e.printStackTrace();
        } catch (FactoryException ex){
            ex.printStackTrace();
        }
        return null;
    }
}
