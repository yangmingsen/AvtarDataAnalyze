package top.ccw.avtar.websocket;

public class WebSocketClient {

    private static MessageServiceWebSocket socketClient = null;

    public static void initClient(MessageServiceWebSocket client) {
        socketClient = client;
        socketClient.connect();
    }

    public static void sendMsg() {
        socketClient.send("ok");
    }

}
