package org.gislab.hbase.core;

import com.vividsolutions.jts.geom.Coordinate;

/**
 * Created by ladyfish on 2017/5/12.
 */
public interface GeoCode {
    String encode(Coordinate coordinate);
    Coordinate decode(String s);
}
