package top.ccw.avtar.redis;

import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/***
 * Redis连接客户端
 *
 * craeted date: 2019/02/03
 * modify date:
 * @author yangmingsen
 */
public class RedisClient {

    public static String getNowAnalyzeValue() {
        Jedis jedis = new Jedis("39.108.73.162");
        jedis.auth("123");

        String aDirecion = jedis.get("ADirecion");

        return aDirecion;

    }

    public static void main(String[] args) {
        System.out.println("dir = "+getNowAnalyzeValue());
    }


}
