package org.gislab.hbase.index;

/**
 * Created by ladyfish on 2017/5/12.
 */
public class IndexFactory {
    public static GridIndex getGridIndex(double tlX,double tlY,double px,double py){
        return new GridIndex(tlX,tlY,px,py);
    }

    public static GridIndex getGridIndex(double tlX,double tlY,double px,double py,int xSize,int ySize){
        return new GridIndex(tlX,tlY,px,py,xSize,ySize);
    }
    public static MutiGridIndex getMutiGridIndex(){
        return new MutiGridIndex();
    }

    public static QuadTreeIndex getQuadTreeIndex(){
        return new QuadTreeIndex();
    }
    public static RTreeIndex getRTreeIndex(){
        return new RTreeIndex();
    }

}
