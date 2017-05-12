package org.gislab.hbase.entity;

import com.vividsolutions.jts.geom.Envelope;

import java.io.Serializable;

/**
 * Created by ladyfish on 2017/5/12.
 */
public class Grid extends Envelope implements Serializable{
    private int xIndex;
    private int yIndex;
    public Grid(double x1, double x2, double y1, double y2,int xIndex,int yIndex){
        super(x1,x2,y1,y2);
        this.xIndex=xIndex;
        this.yIndex=yIndex;
    }
    public int getxIndex() {
        return xIndex;
    }

    public int getyIndex() {
        return yIndex;
    }
}
