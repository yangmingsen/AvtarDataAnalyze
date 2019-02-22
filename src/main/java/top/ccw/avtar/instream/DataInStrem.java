package top.ccw.avtar.instream;


/***
 * Kafka数据读取程序存储程序：这个程序只负责监听kafka中的数据来源。
 * 一旦数据来了，存入HBASE 和 MySQL数据库。并发一个通知消息给Kafka。
 */
public class DataInStrem implements Runnable {
    @Override
    public void run() {
        while (true) {
            System.out.println("DataInStrem ..... "+Thread.currentThread().getName()+"---------\n");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
