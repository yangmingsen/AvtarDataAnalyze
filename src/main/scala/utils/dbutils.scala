package utils

import java.sql._

/**
  * created by ljq on 19-3-5.
  *
  **/
object dbutils {
  val Driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://rm-uf6871zn4f8aq9vpvro.mysql.rds.aliyuncs.com:3306/job_data?useUnicode=true&characterEncoding=utf8" +
    "&useSSL=false"
  val user = "user"
  val pwd = "Group1234"

  def getconn(): Connection = {
    var conn: Connection = null
    try {
      Class.forName(Driver)
      conn = DriverManager.getConnection(url, user, pwd)
    }
    catch {
      case e: ClassNotFoundException => e.printStackTrace
      case e: SQLException => e.printStackTrace
    }
    conn
  }

  def close(rs: ResultSet, psmt: PreparedStatement, conn: Connection): Unit = {
    if (rs != null) {
      rs.close()
    }
    if (psmt != null) {
      psmt.close()
    }
    if (conn != null) {
      conn.close()
    }
  }

  def close(psmt: PreparedStatement, conn: Connection): Unit = {
    if (psmt != null) {
      psmt.close()
    }
    if (conn != null) {
      conn.close()
    }
  }

  def update_statistical(dbname: String, result: String, time: String, jobtype_two_id: String): Unit = {
    var conn: Connection = null
    var ps: java.sql.PreparedStatement = null
    try {
      conn = getconn()
      ps = conn.prepareStatement("update " + dbname + " set result=? where time='" + time + "' AND jobtype_two_id='" + jobtype_two_id + "'")
      ps.setString(1, result)
      ps.executeUpdate()
    }
    catch {
      case e: Exception => e.printStackTrace
    }
    close(ps, conn)
  }


  def insert_statistical(dbname: String, result: String, jobtype_two_id: String): Unit = {
    var conn: Connection = null
    var ps: java.sql.PreparedStatement = null
    try {
      conn = getconn()
      ps = conn.prepareStatement("insert into " + dbname + "(jobtype_two_id,result,time) VALUES(?,?,CURDATE())")
      ps.setString(1, jobtype_two_id)
      ps.setString(2, result)
      ps.executeUpdate()
    }
    catch {
      case e: Exception => e.printStackTrace
    }
    close(ps, conn)
  }

  def judge_statistical(dbname: String, time: String, jobtype_two_id: String): Boolean = {
    var conn: Connection = null
    var ps: java.sql.PreparedStatement = null
    var rs: ResultSet = null
    var result: Boolean = false
    try {
      conn = getconn()
      ps = conn.prepareStatement("SELECT time from " + dbname + " where time = '" + time + "' AND jobtype_two_id='" + jobtype_two_id + "'")
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
    close(rs, ps, conn)
    result
  }
}
