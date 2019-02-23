
class Queue {
    private String name;
    private Object content;

    public Queue(String name, Object content) {
        this.name = name;
        this.content = content;
    }


    public Object getContent() {
        return content;
    }

    public void setContent(Object content) {
        this.content = content;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    public void setContent(String content) {
        this.content = content;
    }


    @Override
    public String toString() {
        return "QueueCmd{" +
                "name='" + name + '\'' +
                ", content='" + content + '\'' +
                '}';
    }

}

public class Test {
    public static void main(String[] args) {
        Queue queue = new Queue("A",new String("ok"));
        System.out.println(queue.getContent());
    }
}
