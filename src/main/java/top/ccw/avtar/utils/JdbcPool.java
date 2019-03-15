package top.ccw.avtar.utils;

import javax.sql.DataSource;

import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedList;
import java.util.Properties;
import java.util.logging.Logger;

public class JdbcPool implements DataSource {

    /**
     * @Field: Connections
     *         使用Vector来存放数据库链接，
     *         Vector具备线程安全性
     */
    // private static Vector Connections = new Vector();
    private static LinkedList<Connection> Connections=new LinkedList<Connection>();

    static{
        //在静态代码块中加载db.properties数据库配置文件
        // InputStream in = JdbcPool.class.getClassLoader().getResourceAsStream("src/main/resources/application.properties");

        InputStream in = JdbcPool.class.getClassLoader().getResourceAsStream("application.properties");
        Properties prop = new Properties();
        try {
            prop.load(in);
            String driver = prop.getProperty("driver");
            String url = prop.getProperty("url");
            String username = prop.getProperty("username");
            String password = prop.getProperty("password");
            //数据库连接池的初始化连接数大小
            int jdbcPoolInitSize =Integer.parseInt(prop.getProperty("jdbcPoolInitSize"));
            //加载数据库驱动
            Class.forName(driver);
            for (int i = 0; i < jdbcPoolInitSize; i++) {
                Connection conn = DriverManager.getConnection(url, username, password);
                System.out.println("获取到了链接" + conn);
                //将获取到的数据库连接加入到Connections集合中，Connections此时就是一个存放了数据库连接的连接池
                Connections.add(conn);
            }

            System.out.println("Successful connetcion");

        } catch (Exception e) {
            System.out.println(" 创建数据库连接失败！ " + e.getMessage());
            try {
                throw new SQLException();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }


    public PrintWriter getLogWriter() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }


    public void setLogWriter(PrintWriter out) throws SQLException {
        // TODO Auto-generated method stub

    }


    public void setLoginTimeout(int seconds) throws SQLException {
        // TODO Auto-generated method stub

    }


    public int getLoginTimeout() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }


    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }


    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    /* 获取数据库连接
     * @see javax.sql.DataSource#getConnection()
     */

    public Connection getConnection() throws SQLException {
        //如果数据库连接池中的连接对象的个数大于0
        if (Connections.size()>0) {
            //从Connections集合中获取一个数据库连接
            final Connection conn = (Connection) Connections.removeFirst();
            System.out.println("Connections数据库连接池大小是" + Connections.size());
            //返回Connection对象的代理对象
            return (Connection) Proxy.newProxyInstance(JdbcPool.class.getClassLoader(), conn.getClass().getInterfaces(), new InvocationHandler(){
                public Object invoke(Object proxy, Method method, Object[] args)
                        throws Throwable {
                    if(!method.getName().equals("close")){
                        return method.invoke(conn, args);
                    }else{
                        //如果调用的是Connection对象的close方法，就把conn还给数据库连接池
                        Connections.add(conn);
                        System.out.println(conn + "被还给Connections数据库连接池了！！");
                        System.out.println("Connections数据库连接池大小为" + Connections.size());
                        return null;
                    }
                }
            });
        }else {
            throw new RuntimeException("对不起，数据库忙");
        }
    }


    public Connection getConnection(String username, String password)
            throws SQLException {
        return null;
    }

}
