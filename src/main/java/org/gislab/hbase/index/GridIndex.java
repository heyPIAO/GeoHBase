package org.gislab.hbase.index;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.gislab.hbase.core.GeoHash;
import org.gislab.hbase.entity.Grid;
import org.gislab.hbase.enums.FillingCode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 根据空间图斑的外包矩形构建网格索引，网格索引的
 * Created by ladyfish on 2017/5/11.
 */
class GridIndex implements SpatialIndex,Serializable{
    private double px,py;       //一个单元网格中X,Y方向的长度
    private int xSize,ySize;    //网格中X方向和Y方向的格网数量
    private double tlX,tlY;   //网格左上角点的坐标
    private FillingCode code=null;
    GridIndex(double tlX,double tlY,double px,double py,int xSize,int ySize){
        this.tlX=tlX;
        this.tlY=tlY;
        this.px=px;
        this.py=py;
        this.xSize=xSize;
        this.ySize=ySize;
    }

    GridIndex(double tlX,double tlY,double px,double py){
        this.tlX=tlX;
        this.tlY=tlY;
        this.px=px;
        this.py=py;
    }

    public void setFillingCode(FillingCode code){
        this.code=code;
    }

    public Iterable<Grid> execute(Geometry geometry){
        return execute(geometry.getEnvelopeInternal());
    }

    private Iterable<Grid> execute(Envelope env){
        int xMinIndex=(int)((env.getMinX()-tlX)/px)+1;
        int xMaxIndex=(int)((env.getMaxX()-tlX)/px)+1;
        int yMinIndex=(int)((env.getMinY()-tlY)/py)+1;
        int yMaxIndex=(int)((env.getMaxY()-tlY)/py)+1;
        List<Grid> grids=new ArrayList<>();
        for(int i=xMinIndex;i<=xMaxIndex;i++){
            for(int k=yMinIndex;k<=yMaxIndex;k++){
                grids.add(new Grid((i-1)*px,i*px,(k-1)*py,k*py,i,k));
            }
        }
        return grids;
    }

    private String codeAsPEANO(Grid grid){
        return GeoHash.getInstance().encode(grid.centre());
    }
    private String codeAsHILBERT(Grid grid){
        return null;
    }
    private String codeAsZMIRROR(Grid grid){
        return null;
    }
    public String encoding(Grid grid){
        if(this.code==null){
            return null;
        }else if(this.code==FillingCode.PEANO){
            return codeAsPEANO(grid);
        }else if(this.code==FillingCode.HILBERT){
            return codeAsHILBERT(grid);
        }else if(this.code==FillingCode.ZMIRROR){
            return codeAsZMIRROR(grid);
        }
        return null;
    }


    public Iterable<String> encoding(Iterable<Grid> grids){
        List<String> coding=new ArrayList<>();
        if(this.code==null){
            return null;
        }else if(this.code==FillingCode.PEANO){
           for(Grid g:grids){
               coding.add(codeAsPEANO(g));
           }
        }else if(this.code==FillingCode.HILBERT){
            for(Grid g:grids){
                coding.add(codeAsHILBERT(g));
            }
        }else if(this.code==FillingCode.ZMIRROR){
            for(Grid g:grids){
                coding.add(codeAsZMIRROR(g));
            }
        }
        return coding;
    }
}
