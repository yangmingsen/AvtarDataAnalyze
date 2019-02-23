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

    private static Jedis jedis;

    /**
     * 连接redis服务器
     */
    static {
        jedis=RedisUtil.getJedis();
    }


    /***
     * 存放数据
     * @param key1 Redis key
     * @param key2 HashMap key
     * @param value push value
     */
    public static void putValue(String key1, String key2, String value) {
        Map<String,String> map = new HashMap<String,String>();
        map.put(key2,value);

        jedis.hmset(key1, map);
    }

    /***
     * 根据 key1 与 key2 查询Redis数据
     * @param key1 Redis key
     * @param key2 HashMap key
     * @return
     */
    public static String getValue(String key1, String key2) {
        List<String> hmget = jedis.hmget(key1, key2);

        return hmget.get(0);
    }

}
