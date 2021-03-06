package top.ccw.avtar.start;


import top.ccw.avtar.analyze.AnalyzeData;
import top.ccw.avtar.instream.DataInStrem;
import top.ccw.avtar.task.TimeWorkTask;
import top.ccw.avtar.websocket.MessageServiceWebSocket;
import top.ccw.avtar.websocket.WebSocketClient;

/***
 * 启动程序：启动2个线程（1、Kafka数据读取程序存储程序。2、数据分析程序）
 *
 * @author yangmingsen
 */
public class Start {


    public static void main(String[] args) throws Exception{

        //与springboot服务端建立连接
        WebSocketClient.initClient(new MessageServiceWebSocket("ws://192.168.0.8:8081/bigscreendisplay/websocket/999"));


        //启动分析线程
        Thread analyzeDataThread = new Thread(new AnalyzeData(), "AnalyzeData");
//
//        //启动数据流入线程
//        Thread dataInStremThread = new Thread(new DataInStrem(), "DataInStrem");
//
        analyzeDataThread.start();
//        dataInStremThread.start();


        Thread timeWorkTaskThread = new Thread(new TimeWorkTask(),"TimeWorkTask");
        timeWorkTaskThread.start();


        while (true) {
            //System.out.println("main thread ....."+Thread.currentThread().getName()+"---------\n");
            try {
                Thread.sleep(Integer.MAX_VALUE);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
