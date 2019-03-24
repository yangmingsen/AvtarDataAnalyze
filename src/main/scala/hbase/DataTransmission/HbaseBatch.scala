package hbase.DataTransmission

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import entity.JobDataEntity
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.spark.{HBaseContext, KeyFamilyQualifier}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.fs.Path


/**
  * program: AvtarDataAnalyze
  * author: ljq
  * create: 2019-03-18 15:28
  **/
object HbaseBatch {
  var conf: Configuration = _

  //线程池
  lazy val connection: Connection = ConnectionFactory.createConnection(conf)

  def MysqlToHBaseStart(jobsRDD: RDD[JobDataEntity]): Unit = {
    val conf1 = new SparkConf().setAppName("HbaseBatch").set("spark.driver.host", "localhost").setMaster("local[*]")
    val sc = new SparkContext(conf1)
    setConf("master,machine1,machine2", "2181", "master")
    val hbaseContext = new HBaseContext(sc, conf)
    val tableNameStr = "tb_job_data"
    val loadPathStr = "/hbase/data1"
    val table = connection.getTable(TableName.valueOf(tableNameStr))
    val rdd = sc.parallelize(MysqlToHBase.getHbaseDomainArray(jobsRDD))
    rdd.hbaseBulkLoad(hbaseContext, TableName.valueOf(tableNameStr),
      t => {
        val rowKey = t._1
        val family: Array[Byte] = t._2(0)._1
        val qualifier = t._2(0)._2
        val value = t._2(0)._3
        val keyFamilyQualifier = new KeyFamilyQualifier(rowKey, family, qualifier)
        Seq((keyFamilyQualifier, value)).iterator
      },
      loadPathStr)
    val load = new LoadIncrementalHFiles(conf)
    load.doBulkLoad(new Path(loadPathStr), connection.getAdmin, table, connection.getRegionLocator(TableName.valueOf(tableNameStr)))
    this.close()
  }

  def setConf(quorum: String, port: String, hbase_pos: String): Unit = {
    //System.setProperty("Hadoop_USER_NAME","root")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.rootdir", "hdfs://" + hbase_pos + ":8020/hbase")
    conf.set("hbase.zookeeper.quorum", quorum)
    conf.set("hbase.zookeeper.property.clientPort", port)
    this.conf = conf
  }

  def close(): Unit = {
    connection.close()
  }
}