package utils

import java.sql.{Connection, DriverManager, ResultSet}

/**
  * created by ljq on 19-3-5.
  *
  **/
object dbutils {
  val Driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com:3306/job_data?useUnicode=true&characterEncoding=utf8" +
    "&useSSL=false"

  def update_statistical(dbname: String, result: String, time: String): Unit = {
    var conn: Connection = null
    var ps: java.sql.PreparedStatement = null
    try {
      Class.forName(Driver)
      conn = DriverManager.getConnection(url, "user", "Group1234")
      ps = conn.prepareStatement("update " + dbname + " set result=? where time='" + time + "'")
      ps.setString(1, result)
      ps.executeUpdate()
    }
    catch {
      case e: Exception => e.printStackTrace
    }
    conn.close()
    ps.close()
  }


  def insert_statistical(dbname: String, result: String, jobtype_two_id: String): Unit = {
    var conn: Connection = null
    var ps: java.sql.PreparedStatement = null
    try {
      Class.forName(Driver)
      conn = DriverManager.getConnection(url, "user", "Group1234")
      ps = conn.prepareStatement("insert into " + dbname + "(jobtype_two_id,result,time) VALUES(?,?,CURDATE())")
      ps.setString(1, jobtype_two_id)
      ps.setString(2, result)
      ps.executeUpdate()
    }
    catch {
      case e: Exception => e.printStackTrace
    }
    conn.close()
    ps.close()
  }

  def judge_statistical(dbname: String, time: String): Boolean = {
    var conn: Connection = null
    var ps: java.sql.PreparedStatement = null
    var rs: ResultSet = null
    var result: Boolean = false
    try {
      Class.forName(Driver)
      conn = DriverManager.getConnection(url, "user", "Group1234")
      ps = conn.prepareStatement("SELECT time from " + dbname + " where time = '" + time + "'")
      rs = ps.executeQuery()
      if (rs.next()) {
        result = false
      }
      else
        result = true
    }
    catch {
      case e: Exception => e.printStackTrace
    }
    conn.close()
    ps.close()
    rs.close()
    result
  }
}
