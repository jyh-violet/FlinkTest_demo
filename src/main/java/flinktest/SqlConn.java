package flinktest;

import javax.swing.plaf.synth.SynthEditorPaneUI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlConn {

    // configuration of the mysql
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost:3306/test?user=root";
    static final String USER = "";
    static final String PASS = "";

    static Connection connection = null;

    public static Connection getConnection()
    {
        if(connection == null)
        {
            try {
                Class.forName("com.mysql.jdbc.Driver");
                connection = DriverManager.getConnection(DB_URL);
                System.out.println("mysql!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public static void runSql(String sql)
    {
        Connection conn = getConnection();
 //       System.out.println(sql);
        try {
            Statement stat = conn.prepareStatement(sql);
            stat.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void Close()
    {
        if(connection != null)
        {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
