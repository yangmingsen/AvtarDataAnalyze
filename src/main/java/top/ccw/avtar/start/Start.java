package top.ccw.avtar.start;


import top.ccw.avtar.analyze.AnalyzeData;
import top.ccw.avtar.instream.DataInStrem;

/***
 * 启动程序：启动2个线程（1、Kafka数据读取程序存储程序。2、数据分析程序）
 */
public class Start {


    public static void main(String[] args) {
        Thread analyzeData = new Thread(new AnalyzeData(), "AnalyzeData");
        Thread dataInStrem = new Thread(new DataInStrem(), "DataInStrem");

        analyzeData.start();
        dataInStrem.start();


        while (true) {
            System.out.println("main thread ....."+Thread.currentThread().getName()+"---------\n");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
