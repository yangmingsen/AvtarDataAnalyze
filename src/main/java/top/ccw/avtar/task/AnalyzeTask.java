package top.ccw.avtar.task;

import spark.rdd.ExcuteAnalyze;
import top.ccw.avtar.entity.QueueCmd;
import utils.LoggerLevels;

public class AnalyzeTask implements Runnable {

    private QueueCmd queueCmd ;

    public AnalyzeTask(QueueCmd queueCmd) {
        this.queueCmd = queueCmd;
    }

    @Override
    public void run() {
        LoggerLevels.setStreamingLogLevels();
        ExcuteAnalyze.startAnalyze(this.queueCmd.getContent().toString());
    }
}
