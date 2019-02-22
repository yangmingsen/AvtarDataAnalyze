package top.ccw.avtar.analyze;

/***
 * 数据分析程序：当程序收到kafka的消息（1、数据来了的通知消息。2、移动端的指令消息），
 * 并根据消息的指令，启动分析分析程序。
 */
public class AnalyzeData implements Runnable{
    @Override
    public void run() {
        System.out.println("AnalyzeData ..... "+Thread.currentThread().getName()+"---------\n");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
