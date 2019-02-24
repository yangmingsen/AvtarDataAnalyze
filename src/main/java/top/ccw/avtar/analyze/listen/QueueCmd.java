package top.ccw.avtar.analyze.listen;

/***
 * 命令对象
 * 其中name 表示的是当前命令名字
 *      content 表示命令内容
 *
 * @author yangmingsen
 */
public class QueueCmd {
    private String name;
    private Object content;

    public QueueCmd(String name, Object content) {
        this.name = name;
        this.content = content;
    }

    public Object getContent() {
        return content;
    }

    public void setContent(Object content) {
        this.content = content;
    }

    public QueueCmd() { }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }




    @Override
    public String toString() {
        return "QueueCmd{" +
                "name='" + name + '\'' +
                ", content='" + content + '\'' +
                '}';
    }


}
