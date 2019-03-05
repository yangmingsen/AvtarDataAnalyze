package utils

import java.sql.{Connection, DriverManager}

/**
  * created by ljq on 19-3-5.
  *
  **/
object dbutils {
  def insert(result: String,dbname:String): Unit = {
    val Driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com:3306/job_data?useUnicode=true&characterEncoding=utf8" +
      "&useSSL=false"
    var conn: Connection = null
    var ps: java.sql.PreparedStatement = null
    val sql = "insert into "+dbname+"(result,jobtype_two_id,column_id) values(?,?,?)"
    try {
      Class.forName(Driver)
      conn = DriverManager.getConnection(url, "user", "Group1234")
      ps = conn.prepareStatement(sql)
      ps.setString(1, result)
      ps.setInt(2, 10)
      ps.setInt(3, 9)
      ps.executeUpdate()
    }
    catch {
      case e: Exception => e.printStackTrace
    }
    conn.close()
    ps.close()
  }

}
