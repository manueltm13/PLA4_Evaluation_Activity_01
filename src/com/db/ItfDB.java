package com.db;

import java.util.ArrayList;

/**
 *
 * @author manuel
 */
public interface ItfDB {
    int insert(String table, ArrayList<String> values);
    int delete(String table, String where);
    int update(String table, String where, String values);
    ArrayList<String> select(String table, String where);
    ArrayList<String> getTables();
    ArrayList<String[]> getColumns(String table);
    String getErrorMsg();
}
