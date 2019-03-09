package top.ccw.avtar.analyze.listen;


import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.MessageListener;
import javax.jms.Message;

import com.google.gson.Gson;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;


/***
 * ActiveMQ工具：
 * 监听数据类型为 MapMessage
 * @author yangmingsen
 */
public class ConsumerTool implements MessageListener,ExceptionListener {
    private String user = ActiveMQConnection.DEFAULT_USER;
    private String password = ActiveMQConnection.DEFAULT_PASSWORD;
    private String url = ActiveMQConnection.DEFAULT_BROKER_URL;
    private String subject = "AnalyzeCmd";
    private Destination destination = null;
    private Connection connection = null;
    private Session session = null;
    private MessageConsumer consumer = null;
    private ActiveMQConnectionFactory connectionFactory=null;
    public static Boolean isconnection=false;
    // 初始化
    private void initialize() throws JMSException {
        connectionFactory= new ActiveMQConnectionFactory(
                user, password, url);
        connection = connectionFactory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = session.createQueue(subject);
        consumer = session.createConsumer(destination);
    }

    // 消费消息
    public void consumeMessage() throws JMSException {
        initialize();
        connection.start();

        consumer.setMessageListener(this);
        connection.setExceptionListener(this);
        System.out.println("Consumer:->Begin listening...");
        isconnection=true;
        // 开始监听
        Message message = consumer.receive();
        System.out.println(message.getJMSMessageID());
        //System.out.println("---------");
        this.onMessage(message);
    }

    // 关闭连接
    public void close() throws JMSException {
        System.out.println("Consumer:->Closing connection");
        if (consumer != null)
            consumer.close();
        if (session != null)
            session.close();
        if (connection != null)
            connection.close();
    }



    /***
     * 处理来自ActiveMQ的消息，
     * @param message
     */
    public void onMessage(Message message) {

        try {

            if(message instanceof MapMessage){

                MapMessage mm = (MapMessage) message;

                //获取主题信息
                String listenCmdStr = mm.getString("QueueCmd");
                Gson gson = new Gson();

                //获取命令对象 通过gson
                /**
                 * ActiveMQ发给分析程序的数据类型为MapMessage 为队列模式。
                 * queue名字（desitination）为：/AnalyzeCmd 数据(MapMessage=>ListenCmd).
                 *  命令内容：
                 * 1、QueueCmd（name="A",content="null")表示来自有数据来了的通知消息
                 * 2、QueueCmd（name="B",content="方向指令")表示移动端发来指令的通知消息
                 */
                QueueCmd listenCmd = gson.fromJson(listenCmdStr, QueueCmd.class);

                //1.如果是 name = A
                if(listenCmd.getName().equals("A")) {
                    //do execute things....

                } else if (listenCmd.getName().equals("B")) {//2.如果 name = B
                    //do execute things....

                }



            } else {
                System.out.println("Others info => Consumer:->Received: " + message);
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void onException(JMSException arg0){
        isconnection=false;
    }

}
