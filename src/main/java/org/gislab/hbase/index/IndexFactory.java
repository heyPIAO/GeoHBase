package org.gislab.hbase.index;

/**
 * Created by ladyfish on 2017/5/12.
 */
public class IndexFactory {
    public static GridIndex getGridIndex(){
        return new GridIndex();
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
