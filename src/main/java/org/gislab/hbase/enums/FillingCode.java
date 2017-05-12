package org.gislab.hbase.enums;

/**
 * 定义了空间的降维类型
 * Created by ladyfish on 2017/5/11.
 */
public enum FillingCode {
    PEANO,              //GeoHash选择的填充曲线，更适合点数据
    HILBERT,            //相对于其他曲线减少了突变情况的发生，是和线和面数据
    ZMIRROR
}
