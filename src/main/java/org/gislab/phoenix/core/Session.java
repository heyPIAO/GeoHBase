package org.gislab.phoenix.core;

import java.io.Closeable;
import java.io.IOException;
import java.sql.*;

/**
 * 实现一个连接Phoenix的会话
 * Created by ladyfish on 2017/5/11.
 */
public class Session implements ISession,Closeable {
    private Connection connection;

    private Session(){}

    /**
     * 获取一个Session对话
     * @return
     */
    public static Session getSession(){
        return new Session();
    }

    /**
     * 关闭Session对话
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        try {
            if(connection!=null&&!connection.isClosed()){
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /**
     * 连接Phoenix数据库
     * @param connStr example："jdbc:phoenix:[zookeeper]"
     */
    @Override
    public void doConnection(String connStr) {
        try {
            connection=DriverManager.getConnection(connStr);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 执行SQL操作
     * @param sql
     */
    @Override
    public void doExecute(String sql) {
        if(connection!=null){
            try {
                Statement statement=connection.createStatement();
                statement.executeUpdate(sql);
                connection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 批量执行SQL操作
     * @param sqls
     */
    public void doExecute(String[] sqls){
        try {
            if(connection!=null&&connection.isClosed()){
                Statement statement=connection.createStatement();
                for(String sql:sqls){
                    statement.executeUpdate(sql);
                }
                connection.commit();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /**
     * 执行查询操作
     * @param sql
     * @return
     */
    @Override
    public ResultSet doQuery(String sql) {
        ResultSet rs=null;
        try {
            if (connection!=null&&!connection.isClosed()){
                PreparedStatement statement=connection.prepareStatement(sql);
                rs=statement.executeQuery();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }
}
