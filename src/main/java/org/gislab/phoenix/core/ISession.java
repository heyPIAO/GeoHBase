package org.gislab.phoenix.core;

import java.sql.Connection;
import java.sql.ResultSet;

/**
 * Created by ladyfish on 2017/5/11.
 */
public interface ISession {
    void doConnection(String connStr);
    void doExecute(String sql);
    ResultSet doQuery(String sql);

}
