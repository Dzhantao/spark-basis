package SparkSQLDemo

import java.sql.DriverManager

/**
  * Created by Dzhantao on 2018/5/23.
  * 通过JDBC的方式访问hive
  */
object SparkSQLThriftServerApp {
    def main(args: Array[String]): Unit = {
        Class.forName("org.apache.hive.jdbc.HiveDriver")
        val conn = DriverManager.getConnection("jdbc:hive2://192.168.44.10:10000/default","","")
        val pstmt = conn.prepareStatement("select * from mv limit 10")
        val rs = pstmt.executeQuery()
        while (rs.next()){
            println("name is" + rs.getString("name"))
        }
        rs.close()
        pstmt.close()
        conn.close()
    }
}
