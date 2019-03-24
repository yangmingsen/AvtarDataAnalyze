package utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable

/**
  * program: AvtarDataAnalyze
  * author: ljq
  * create: 2019-03-17 21:55
  **/
object HbaseUtils {
  var conf: Configuration = _

  //线程池
  lazy val connection: Connection = ConnectionFactory.createConnection(conf)
  lazy val admin: Admin = connection.getAdmin

  /**
    * hbase conf
    *
    * @param quorum hbase的zk地址
    * @param port   zk端口2181
    * @return
    */
  def setConf(quorum: String, port: String,hbase_pos:String): Unit = {
    //System.setProperty("Hadoop_USER_NAME","root")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.rootdir", "hdfs://" + hbase_pos+ ":8020/hbase")
    conf.set("hbase.zookeeper.quorum", quorum)
    conf.set("hbase.zookeeper.property.clientPort", port)
    this.conf = conf
  }

  /**
    * 如果不存在就创建表
    *
    * @param tableName    命名空间：表名
    * @param columnFamily 列族
    */
  def createTable(tableName: String, columnFamily: String): Unit = {
    val tbName = TableName.valueOf(tableName)
    if (!admin.tableExists(tbName)) {
      val htableDescriptor = new HTableDescriptor(tbName)
      val hcolumnDescriptor = new HColumnDescriptor(columnFamily)
      htableDescriptor.addFamily(hcolumnDescriptor)
      admin.createTable(htableDescriptor)
    }
  }

  def hbaseScan(tableName: String): ResultScanner = {
    val scan = new Scan()
    val table = connection.getTable(TableName.valueOf(tableName))
    table.getScanner(scan)
    //val scanner: CellScanner = rs.next().cellScanner()
  }

  /**
    * 获取hbase单元格内容
    *
    * @param tableName 命名空间：表名
    * @param rowKey    rowkey
    * @return 返回单元格组成的List
    */
  def getCell(tableName: String, rowKey: String): mutable.Buffer[Cell] = {
    val get = new Get(Bytes.toBytes(rowKey))
    /*if (qualifier == "") {
      get.addFamily(family.getBytes())
    } else {
      get.addColumn(family.getBytes(), qualifier.getBytes())
    }*/
    val table = connection.getTable(TableName.valueOf(tableName))
    val result: Result = table.get(get)
    import scala.collection.JavaConverters._
    result.listCells().asScala
    /*.foreach(cell=>{
    val rowKey=Bytes.toString(CellUtil.cloneRow(cell))
    val timestamp = cell.getTimestamp;  //取到时间戳
    val family = Bytes.toString(CellUtil.cloneFamily(cell))  //取到族列
    val qualifier  = Bytes.toString(CellUtil.cloneQualifier(cell))  //取到修饰名
    val value = Bytes.toString(CellUtil.cloneValue(cell))
    println(rowKey,timestamp,family,qualifier,value)
  })*/
  }

  /**
    * 单条插入
    * @param tableName 命名空间：表名
    * @param rowKey    rowkey
    * @param family    列族
    * @param qualifier column列
    * @param value     列值
    */
  def singlePut(tableName: String, rowKey: String, family: String, qualifier: String, value: String): Unit = {
    //向表中插入数据
    //单个插入
    val put: Put = new Put(Bytes.toBytes(rowKey)) //参数是行键row01
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value))
    //获得表对象
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    table.put(put)
    table.close()
  }

  /**
    * 删除数据
    *
    * @param tbName 表名
    * @param row    rowkey
    */
  def deleteByRow(tbName: String, row: String): Unit = {
    val delete = new Delete(Bytes.toBytes(row))
    // delete.addColumn(Bytes.toBytes("fm2"), Bytes.toBytes("col2"))
    val table = connection.getTable(TableName.valueOf(tbName))
    table.delete(delete)
  }

  def close(): Unit = {
    admin.close()
    connection.close()
  }

  def main(args: Array[String]): Unit = {
    setConf("master,machine1,machine2", "2181","master")

    /* import scala.collection.JavaConverters._
     val resultScanner: ResultScanner = hbaseScan("tb_job_data")
     resultScanner.asScala.foreach(rs=>{
       val cells = rs.listCells()
       cells.asScala.foreach(cell => {
         val rowKey = Bytes.toString(CellUtil.cloneRow(cell))
         val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell)) //取到修饰名
         val value = Bytes.toString(CellUtil.cloneValue(cell))
         println(rowKey,qualifier,value)
       })
     })*/
    //createTable("tb_job_data", "info")
    //singlePut("tb_job_data", "2","info","haha","6")
    //deleteByRow("tb_job_data","1")
    //getCell("tb_job_data", "1")
    this.close()
  }
}
