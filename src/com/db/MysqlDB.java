package com.db;

import java.sql.*;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author manuel
 */

public class MysqlDB implements ItfDB{
    private final String URL = "jdbc:mysql://localhost:3306/";
    private final String BDD = "tm13ventas";
    private final String USER = "root";
    private final String PASSWORD = "";    
    private Connection cnn;
    private String errorMsg = "";
    
    private void dbConnect() throws ClassNotFoundException, SQLException{
        Class.forName("com.mysql.jdbc.Driver");
        setCnn(DriverManager.getConnection(getURL() + getBDD(), getUSER(), getPASSWORD()));
    }

    /**
     * Execute a sql "insert" statetment for a record
     * @param table - Table or tables where to update
     * @param values - The values for the record attributes
     * @return - The number of records updated or a negative value on error
     */
    @Override
    public int insert(String table, ArrayList<String> values) {
        PreparedStatement pstm = null;
        String sqlValues = "";
        int cnt;
        
        try{
            dbConnect();
            for(String field: values)
                if(sqlValues.equals(""))
                    sqlValues += "?";
                else
                    sqlValues += ", ?";
            sqlValues = " values(" + sqlValues + ")";
            pstm = getCnn().prepareStatement("insert into " + table + sqlValues);
            for(int i = 0; i < values.size(); i++)
                if(values.get(i) == null)
                    pstm.setNull(i + 1, java.sql.Types.INTEGER);
                else
                    pstm.setString(i + 1, values.get(i));
            pstm.executeUpdate();
            cnt = pstm.getUpdateCount();
            pstm.close();
            return cnt;
        }catch(ClassNotFoundException | SQLException ex){
            if(pstm == null)
                setErrorMsg("insert\n" + ex + ".\n" + table + ". " + sqlValues);
            else
                setErrorMsg("insert\n" + ex + ".\n" + pstm.toString());
            return -99;
        }finally{
            try {
                getCnn().close();
            } catch (SQLException ex) {
                Logger.getLogger(MysqlDB.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * Execute a sql "delete" statetment
     * @param table - Table or tables where to delete records
     * @param where - Where clause of the statement
     * @return - The number of records deleted or a negative value on error
     */
    @Override
    public int delete(String table, String where) {
        PreparedStatement pstm = null;
        int cnt;
        
        try{
            dbConnect();
            pstm = getCnn().prepareStatement("delete from " + table + " where " + where);
            pstm.executeUpdate();
            cnt = pstm.getUpdateCount();
            pstm.close();
            return cnt;
        }catch(ClassNotFoundException | SQLException ex){
            if(pstm == null)
                setErrorMsg("delete\n" + ex + ".\n" + table + ". " + where);
            else
                setErrorMsg("delete\n" + ex + ".\n" + pstm.toString());
            return -99;
        }finally{
            try {
                getCnn().close();
            } catch (SQLException ex) {
                Logger.getLogger(MysqlDB.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * Execute a sql "update" statetment
     * @param table - Table or tables where to update
     * @param where - Where clause of the statement
     * @param values - Set clause of the statement
     * @return - The number of records updated or a negative value on error
     */
    @Override
    public int update(String table, String where, String values) {
        PreparedStatement pstm = null;
        int cnt;

        try{
            dbConnect();
            pstm = getCnn().prepareStatement("update " + table + " set " + values + " where " + where);
            pstm.executeUpdate();
            cnt = pstm.getUpdateCount();
            pstm.close();
            return cnt;
        }catch(ClassNotFoundException | SQLException ex){
            if(pstm == null)
                setErrorMsg("update\n" + ex + ".\n" + table + ". " + where + ". " + values);
            else
                setErrorMsg("update\n" + ex + ".\n" + pstm.toString());
            return -99;
        }finally{
            try {
                getCnn().close();
            } catch (SQLException ex) {
                Logger.getLogger(MysqlDB.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * Execute a sql "select" statetment
     * @param table - Table or tables where to select
     * @param where - Where clause of the statement
     * @return - An ArrayList of Strings with the selected attributes separated 
     *      with " | " or null on error
     */
    @Override
    public ArrayList<String> select(String table, String where) {
        PreparedStatement pstm = null;
        ResultSet rs;
        ResultSetMetaData rsmd;
        String line = "";
        ArrayList<String> output = new ArrayList<>();

        try{
            dbConnect();
            pstm = getCnn().prepareStatement("select * from " + table + " where " + where);
            rs = pstm.executeQuery();
            rsmd = rs.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++)
                if(line.equals(""))
                    line += rsmd.getColumnName(i);
                else
                    line += " | " + rsmd.getColumnName(i);
            output.add(line);
            while(rs.next()){
                line = "";
                for(int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
                    if(line.equals(""))
                        line += rs.getString(i);
                    else
                        line += " | " + rs.getString(i);
                output.add(line);
            }
            rs.close();
            pstm.close();
            return output;
        }catch(ClassNotFoundException | SQLException ex){
            if(pstm == null)
                setErrorMsg("select\n" + ex + ".\n" + table + ". " + where);
            else
                setErrorMsg("select\n" + ex + ".\n" + pstm.toString());
            return null;
        }finally{
            try {
                getCnn().close();
            } catch (SQLException ex) {
                Logger.getLogger(MysqlDB.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * Get the table names in the schema whithout the "tbl_" prefix
     * @return - The table names whithout the "tbl_" prefix
     */
    @Override
    public ArrayList<String> getTables() {
        PreparedStatement pstm = null;
        ResultSet rs;
        String tableName;
        ArrayList<String> output = new ArrayList<>();

        try{
            dbConnect();
            pstm = getCnn().prepareStatement("show tables");
            rs = pstm.executeQuery();
            while (rs.next()){
                tableName = rs.getString(1);
                if(tableName.toUpperCase().startsWith("TBL_"))
                    output.add(tableName.substring(4));
            }
            rs.close();
            pstm.close();
            return output;
        }catch(ClassNotFoundException | SQLException ex){
            if(pstm == null)
                setErrorMsg("getEditableTables\n" + ex.toString());
            else
                setErrorMsg("getEditableTables\n" + ex + ".\n" + pstm.toString());
            return null;
        }finally{
            try {
                getCnn().close();
            } catch (SQLException ex) {
                Logger.getLogger(MysqlDB.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * Get the attributes information from a given table
     * @param table - The table
     * @return - The table attributes information 
     */
    @Override
    public ArrayList<String[]> getColumns(String table) {
        PreparedStatement pstm = null;
        ResultSet rs;
        ResultSetMetaData rsmd ;
        ArrayList<String[]> output = new ArrayList<>();
        ArrayList<String> key = new ArrayList<>();
        String field[], type;

        try{
            dbConnect();
            rs = getCnn().getMetaData().getPrimaryKeys(null, getCnn().getSchema(), table);
            while (rs.next())
                key.add(rs.getString("COLUMN_NAME"));
            rs.close();
            pstm = getCnn().prepareStatement("select * from " + table + " limit 1");
            rs = pstm.executeQuery();
            rsmd = rs.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++){
                field = new String[] {rsmd.getColumnName(i), rsmd.getColumnTypeName(i)};
                if(key.contains(field[0]))
                    field[1] = field[1] + ", PK"; 
                if(rsmd.isNullable(i) == 0)
                    field[1] = field[1] + ", NOTNULL";
                if(rsmd.isAutoIncrement(i))
                    field[1] = field[1] + ", AUTOINCREMENT"; 
                if(rsmd.isReadOnly(i))
                    field[1] = field[1] + ", READONLY";
                output.add(field);
            }
            rs.close();
            pstm.close();
            return output;
        }catch(ClassNotFoundException | SQLException ex){
            if(pstm == null)
                setErrorMsg("getColumns\n" + ex + ".\n" + table);
            else
                setErrorMsg("getColumns\n" + ex + ".\n" + pstm.toString());
            return null;
        }finally{
            try {
                getCnn().close();
            } catch (SQLException ex) {
                Logger.getLogger(MysqlDB.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * cnn getter
     * @return the cnn
     */
    private Connection getCnn() {
        return cnn;
    }

    /**
     * cnn setter
     * @param cnn the cnn to set 
     */
    private void setCnn(Connection cnn) {
        this.cnn = cnn;
    }

    /**
     * errorMsg getter
     * @return the errorMsg
     */
    @Override
    public String getErrorMsg() {
        return errorMsg;
    }

    /**
     * errorMsg setter
     * @param errorMsg the errorMsg to set 
     */
    private void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    /**
     * URL getter
     * @return the URL
     */
    public String getURL() {
        return URL;
    }

    /**
     * BDD getter
     * @return the BDD
     */
    public String getBDD() {
        return BDD;
    }

    /**
     * USER getter
     * @return the USER
     */
    public String getUSER() {
        return USER;
    }

    /**
     * PASSWORD getter
     * @return the PASSWORD
     */
    public String getPASSWORD() {
        return PASSWORD;
    }
    
}
