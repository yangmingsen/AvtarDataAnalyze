package top.ccw.avtar.websocket;

public class TestWebSocket  {

    public static void main(String[] args) throws Exception {

        WebSocketClient.initClient(new MessageServiceWebSocket("ws://192.168.0.8:8081/bigscreendisplay/websocket/999"));

        for (int i = 0; i < 10; i++) {

            try {
                Thread.sleep(5*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            WebSocketClient.sendMsg("9");


        }




    }

}
