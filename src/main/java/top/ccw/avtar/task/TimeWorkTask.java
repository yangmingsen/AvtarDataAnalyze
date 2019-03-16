package top.ccw.avtar.task;

import spark.rdd.ExcuteAnalyze;
import utils.LoggerLevels;

public class TimeWorkTask implements Runnable {
    @Override
    public void run() {
        while (true) {
            LoggerLevels.setStreamingLogLevels();
            ExcuteAnalyze.startAnalyze();

            try {
                Thread.sleep(10*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
