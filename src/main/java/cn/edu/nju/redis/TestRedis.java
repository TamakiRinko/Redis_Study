package cn.edu.nju.redis;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.hash.Jackson2HashMapper;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Component
public class TestRedis {
//    // 这个找不到Autowired的对象！没有RedisTemplate<String, Object>类型的Bean
//    @Autowired
//    RedisTemplate<String, Object> redisTemplate;
//    // 这个自动匹配到StringRedisTemplate上了！
//    @Autowired
//    RedisTemplate<String, String> redisTemplate;

    @Autowired
    // 默认使用JdkSerializationRedisSerializer，java序列化！
    RedisTemplate<Object, Object> redisTemplate;
    @Autowired
    StringRedisTemplate stringRedisTemplate;

    @Autowired
    // 用于json解析
    ObjectMapper objectMapper;

    public void testNormal(){
        // high-level
        // Object 序列化出现问题！在redis中key出现乱码
        redisTemplate.opsForValue().set("hello", "hello china");
        System.out.println(redisTemplate.opsForValue().get("hello"));

        // 无序列化问题，本身就是String类型的key，KeySerializer被设置过了！
        stringRedisTemplate.opsForValue().set("hi", "hi china");
        System.out.println(stringRedisTemplate.opsForValue().get("hi"));

        // low-level
        // 直接传入字节数组，不会有序列化问题
        RedisConnection connection = Objects
                .requireNonNull(redisTemplate.getConnectionFactory())
                .getConnection();
        connection.set("hola".getBytes(), "hola china".getBytes());
        System.out.println(new String(Objects.requireNonNull(connection.get("hola".getBytes()))));
    }

    public void testHash(){
        // 全部使用json序列化也可以！
//        redisTemplate.setKeySerializer(new Jackson2JsonRedisSerializer<Object>(Object.class));
//        redisTemplate.setHashKeySerializer(new Jackson2JsonRedisSerializer<Object>(Object.class));
        // 近似变为了RedisTemplate<String, Object>
        redisTemplate.setKeySerializer(RedisSerializer.string());
        redisTemplate.setHashKeySerializer(RedisSerializer.string());
        redisTemplate.setValueSerializer(new Jackson2JsonRedisSerializer<Object>(Object.class));
        redisTemplate.setHashValueSerializer(new Jackson2JsonRedisSerializer<Object>(Object.class));

        HashOperations<Object, Object, Object> hash = redisTemplate.opsForHash();
        // 麻烦！要一个一个传入，想要传入对象！
//        hash.put("111", "key1", "value1");
//        hash.put("111", "key2", "value2");
//        System.out.println(hash.entries("111"));


        // Object对象转为Mapper<String, Object>
        Jackson2HashMapper jackson2HashMapper = new Jackson2HashMapper(objectMapper, false);
        Person person = new Person("zhangsan", 16);
        hash.putAll("person1", jackson2HashMapper.toHash(person));
        Map<Object, Object> map = hash.entries("person1");
        Person person1 = objectMapper.convertValue(map, Person.class);
        // Person(name=zhangsan, age=16)
        System.out.println(person1);
        // {name=zhangsan, age=16}
        System.out.println(hash.entries("person1"));
    }

    public void testPS(){
        // 接收端
        RedisConnection connection = stringRedisTemplate.getConnectionFactory().getConnection();
        connection.subscribe(new MessageListener() {
            @Override
            public void onMessage(Message message, byte[] pattern) {
                System.out.println(new String(message.getBody()));
            }
        }, "ooxx".getBytes());
        while (true){
            // 发送端
            redisTemplate.convertAndSend("ooxx", "111");
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
