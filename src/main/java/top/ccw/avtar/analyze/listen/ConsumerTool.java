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
        System.out.println("Consumer:->Begin listening to ActiveMQ CMD...");
        isconnection=true;
        // 开始监听
        Message message = consumer.receive();
        //System.out.println(message.getJMSMessageID());
        //System.out.println("---------");
        onMessage(message);
    }

    // 关闭连接
    public void close() throws JMSException {
        System.out.println("Consumer:->Closing connection to ActiveMQ CMD...");
        if (consumer != null)
            consumer.close();
        if (session != null)
            session.close();
        if (connection != null)
            connection.close();
    }

    // 消息处理函数

    /***
     * 处理来自ActiveMQ的消息，
     * @param message
     */
    public void onMessage(Message message) {
        try {

            if(message instanceof MapMessage){
                MapMessage mm = (MapMessage) message;

                //获取主题信息
                String listenCmdStr = mm.getString(this.subject);
                Gson gson = new Gson();
                //获取命令对象 通过gson
                QueueCmd listenCmd = gson.fromJson(listenCmdStr, QueueCmd.class);

                //获取命令
                String cmd = (String) listenCmd.getContent();

                //do execute things....


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
