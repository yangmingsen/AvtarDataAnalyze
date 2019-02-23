package top.ccw.avtar.analyze;

import top.ccw.avtar.analyze.listen.ConsumerTool;

/***
 * 数据分析程序：当程序收到kafka的消息（1、数据来了的通知消息。2、移动端的指令消息），
 * 并根据消息的指令，启动分析分析程序。
 */
public class AnalyzeData implements Runnable{

    /***
     *
     */
    @Override
    public void run() {
        System.out.println("Start AnalyzeData Program Thread ..... "+Thread.currentThread().getName()+"---------\n");

        try {

            ConsumerTool consumer = new ConsumerTool();
            consumer.consumeMessage();

            while (ConsumerTool.isconnection) {
                //System.out.println(123);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
