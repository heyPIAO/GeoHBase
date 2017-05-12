package org.gislab.hbase.core;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import org.geotools.geometry.jts.GeometryBuilder;

import java.util.Arrays;

/**
 * Created by ladyfish on 2017/5/12.
 */
public class GeoHash implements GeoCode{

    private static final char[] BASE_32 = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};
    private static final int[] BASE_32_IDX;
    public static final int MAX_PRECISION = 24;
    private static final int[] BITS = new int[]{16, 8, 4, 2, 1};
    private static final double[] hashLenToLatHeight;
    private static final double[] hashLenToLonWidth;
    private static final GeometryBuilder builder=new GeometryBuilder();
    private final static GeoHash helper=new GeoHash();

    public final static GeoHash getInstance(){
        return helper;
    }

    private String encodeLatLon(double latitude, double longitude) {
        return encodeLatLon(latitude, longitude, 12);
    }

    private String encodeLatLon(double latitude, double longitude, int precision) {
        double[] latInterval = new double[]{-90.0D, 90.0D};
        double[] lngInterval = new double[]{-180.0D, 180.0D};
        StringBuilder geohash = new StringBuilder(precision);
        boolean isEven = true;
        int bit = 0;
        int ch = 0;

        while(geohash.length() < precision) {
            double mid = 0.0D;
            if(isEven) {
                mid = (lngInterval[0] + lngInterval[1]) / 2.0D;
                if(longitude > mid) {
                    ch |= BITS[bit];
                    lngInterval[0] = mid;
                } else {
                    lngInterval[1] = mid;
                }
            } else {
                mid = (latInterval[0] + latInterval[1]) / 2.0D;
                if(latitude > mid) {
                    ch |= BITS[bit];
                    latInterval[0] = mid;
                } else {
                    latInterval[1] = mid;
                }
            }

            isEven = !isEven;
            if(bit < 4) {
                ++bit;
            } else {
                geohash.append(BASE_32[ch]);
                bit = 0;
                ch = 0;
            }
        }

        return geohash.toString();
    }

    private Coordinate decodePoint(String geohash) {
        Envelope rect = decodeBoundary(geohash);
        double latitude = (rect.getMinY() + rect.getMaxY()) / 2.0D;
        double longitude = (rect.getMinX() + rect.getMaxX()) / 2.0D;
        return  builder.point(longitude, latitude).getCoordinate();
    }

    private Envelope decodeBoundary(String geohash) {
        double minY = -90.0D;
        double maxY = 90.0D;
        double minX = -180.0D;
        double maxX = 180.0D;
        boolean isEven = true;

        for(int i = 0; i < geohash.length(); ++i) {
            char c = geohash.charAt(i);
            if(c >= 65 && c <= 90) {
                c = (char)(c + 32);
            }

            int cd = BASE_32_IDX[c - BASE_32[0]];
            int[] arr$ = BITS;
            int len$ = arr$.length;

            for(int i$ = 0; i$ < len$; ++i$) {
                int mask = arr$[i$];
                if(isEven) {
                    if((cd & mask) != 0) {
                        minX = (minX + maxX) / 2.0D;
                    } else {
                        maxX = (minX + maxX) / 2.0D;
                    }
                } else if((cd & mask) != 0) {
                    minY = (minY + maxY) / 2.0D;
                } else {
                    maxY = (minY + maxY) / 2.0D;
                }

                isEven = !isEven;
            }
        }
        return builder.box(minX, maxX, minY, maxY).getEnvelopeInternal();
    }

    public String[] getSubGeohashes(String baseGeohash) {
        String[] hashes = new String[BASE_32.length];

        for(int i = 0; i < BASE_32.length; ++i) {
            char c = BASE_32[i];
            hashes[i] = baseGeohash + c;
        }

        return hashes;
    }

    public double[] lookupDegreesSizeForHashLen(int hashLen) {
        return new double[]{hashLenToLatHeight[hashLen], hashLenToLonWidth[hashLen]};
    }

    public int lookupHashLenForWidthHeight(double lonErr, double latErr) {
        for(int len = 1; len < 24; ++len) {
            double latHeight = hashLenToLatHeight[len];
            double lonWidth = hashLenToLonWidth[len];
            if(latHeight < latErr && lonWidth < lonErr) {
                return len;
            }
        }

        return 24;
    }

    static {
        BASE_32_IDX = new int[BASE_32[BASE_32.length - 1] - BASE_32[0] + 1];

        assert BASE_32_IDX.length < 100;

        Arrays.fill(BASE_32_IDX, -500);

        for(int even = 0; even < BASE_32.length; BASE_32_IDX[BASE_32[even] - BASE_32[0]] = even++) {
            ;
        }

        hashLenToLatHeight = new double[25];
        hashLenToLonWidth = new double[25];
        hashLenToLatHeight[0] = 180.0D;
        hashLenToLonWidth[0] = 360.0D;
        boolean var2 = false;

        for(int i = 1; i <= 24; ++i) {
            hashLenToLatHeight[i] = hashLenToLatHeight[i - 1] / (double)(var2?8:4);
            hashLenToLonWidth[i] = hashLenToLonWidth[i - 1] / (double)(var2?4:8);
            var2 = !var2;
        }

    }


    @Override
    public String encode(Coordinate coordinate) {
        return encodeLatLon(coordinate.x,coordinate.y);
    }

    public String encode(Coordinate coordinate,int precision){
        String s = encodeLatLon(coordinate.x, coordinate.y, precision);
        return s;
    }

    @Override
    public Coordinate decode(String s) {
        return decodePoint(s);
    }
}
