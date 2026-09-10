package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{

    private String name;
    private StringRedisTemplate redisTemplate;

    public SimpleRedisLock(String name, StringRedisTemplate redisTemplate) {
        this.name = name;
        this.redisTemplate = redisTemplate;
    }

    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";

//    加载脚本
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT =  new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        // 获取线程标识
        String threaId = ID_PREFIX + Thread.currentThread().getId();
        // 获取锁
        Boolean success = redisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX + name, threaId , timeoutSec, TimeUnit.SECONDS);


        return Boolean.TRUE.equals(success);
    }


    /*
        获取锁标识和释放锁是两步操作，如果获取锁标识后发生了阻塞，会导致两个线程并行
     */
//    @Override
//    public void unlock() {
//        // 获取线程ID标识
//        String threaId = ID_PREFIX + Thread.currentThread().getId();
//        // 获取锁中的标识
//        String id = redisTemplate.opsForValue().get(KEY_PREFIX + name);
//        // 判断标识是否一致
//        if(threaId .equals(id)){
//            // 释放锁
//            redisTemplate.delete(KEY_PREFIX + name);
//        }
//    }

    /**
     * lua脚本
     */
    @Override
    public void unlock() {
        redisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX + name),
                ID_PREFIX + Thread.currentThread().getId());
    }

}
