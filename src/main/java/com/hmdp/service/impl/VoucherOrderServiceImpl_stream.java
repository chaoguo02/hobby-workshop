//package com.hmdp.service.impl;
//
//import cn.hutool.core.bean.BeanUtil;
//import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
//import com.hmdp.dto.Result;
//import com.hmdp.entity.SeckillVoucher;
//import com.hmdp.entity.VoucherOrder;
//import com.hmdp.mapper.VoucherOrderMapper;
//import com.hmdp.service.ISeckillVoucherService;
//import com.hmdp.service.IVoucherOrderService;
//import com.hmdp.utils.RedisIdWorker;
//import com.hmdp.utils.UserHolder;
//import org.redisson.api.RLock;
//import org.redisson.api.RedissonClient;
//import org.springframework.aop.framework.AopContext;
//import org.springframework.core.io.ClassPathResource;
//import org.springframework.data.redis.connection.stream.*;
//import org.springframework.data.redis.core.StringRedisTemplate;
//import org.springframework.data.redis.core.script.DefaultRedisScript;
//import org.springframework.stereotype.Service;
//import org.springframework.transaction.annotation.Transactional;
//
//import javax.annotation.PostConstruct;
//import javax.annotation.Resource;
//import java.time.Duration;
//import java.time.LocalDateTime;
//import java.util.Collections;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ArrayBlockingQueue;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
///**
// * @author 虎哥
// * @since 2021-12-22
// */
//@Service
//public class VoucherOrderServiceImpl_stream extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
//    @Resource
//    private ISeckillVoucherService seckillVoucherService;
//    @Resource
//    private RedisIdWorker redisIdWorker;
//    @Resource
//    private StringRedisTemplate stringRedisTemplate;
//    @Resource
//    private RedissonClient redissonClient;
//
//    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
//    static {
//        SECKILL_SCRIPT =  new DefaultRedisScript<>();
//        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
//        SECKILL_SCRIPT.setResultType(Long.class);
//    }
//
//    private static final BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
//
//    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
//
//    @PostConstruct
//    private void init() {
//        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
//    }
//    private class VoucherOrderHandler implements Runnable {
//        String queueName = "stream.orders";
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    // 获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.order >
//                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
//                            Consumer.from("g1", "c1"),
//                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
//                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
//                    );
//                    // 判断消息获取是否成功
//                    if(list == null || list.isEmpty()){
//                        continue;
//                    }
//                    MapRecord<String, Object, Object> record = list.get(0);
//                    Map<Object, Object> values = record.getValue();
//                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
//                    // 创建订单
//                    handleVoucherOrder(voucherOrder);
//                    // ACK确认 xack streams.order g1 id
//                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
//                }
//                catch (Exception e) {
//                    log.error("订单处理异常",e);
//                    handlePendingList();
//                }
//            }
//        }
//        private void handlePendingList(){
//            while (true) {
//                try {
//                    // 获取pemdinglist中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS streams.order
//                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
//                            Consumer.from("g1", "c1"),
//                            StreamReadOptions.empty().count(1),
//                            StreamOffset.create(queueName, ReadOffset.from("0"))
//                    );
//                    // 判断消息获取是否成功
//                    if(list == null || list.isEmpty()){
//                        break;
//                    }
//                    MapRecord<String, Object, Object> record = list.get(0);
//                    Map<Object, Object> values = record.getValue();
//                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
//                    // 创建订单
//                    handleVoucherOrder(voucherOrder);
//                    // ACK确认 xack streams.order g1 id
//                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
//                }
//                catch (Exception e) {
//                    log.error("订单pending-list处理异常",e);
//                    try {
//                        Thread.sleep(20);
//                    } catch (InterruptedException ex) {
//                        throw new RuntimeException(ex);
//                    }
//                }
//            }
//        }
//    }
//
//
//    private void handleVoucherOrder(VoucherOrder voucherOrder) {
//        Long userId = voucherOrder.getUserId();
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        boolean isLock = lock.tryLock();
//        if (!isLock) {
//            log.error("不允许重复下单");
//            return;
//        }
//        try{
//            proxy.createVoucherOrder(voucherOrder);
//        }finally {
//            lock.unlock();
//        }
//    }
//
//    private IVoucherOrderService proxy;
//
//
////    1、查询优惠券
////    2、判断秒杀是否开始
////    3、判断秒杀是否结束
////    4、判断库存是否充足
////    5、扣减库存
////    6、创建订单
////    7、返回订单id
//
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        Long userId = UserHolder.getUser().getId();
//        long orderId = redisIdWorker.nextId("order");
//        // 1. 执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString(), String.valueOf(orderId)
//        );
//        // 2. 判断结果是否为0
//        int r = result.intValue();
//        if(r != 0){
//            // 没有购买资格
//            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
//        }
//
//        // 有购买资格， 把下单信息保存到阻塞队列
//        VoucherOrder voucherOrder = new VoucherOrder();
//
//        voucherOrder.setId(orderId);
//        voucherOrder.setUserId(userId);
//        voucherOrder.setVoucherId(voucherId);
//        // 放入阻塞队列
//        orderTasks.add(voucherOrder);
//        // 获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        return Result.ok(orderId);
//    }
//
//    @Transactional
//    public void createVoucherOrder(VoucherOrder voucherOrder) {
//        // 5.一人一单(加上悲观锁，给userId加锁)
//        Long userId = voucherOrder.getUserId();
//        int count = query().eq("user_id", userId)
//                .eq("voucher_id", voucherOrder.getVoucherId())
//                .count();
//        if(count > 0){
//            log.error("用户已经购买过一次");
//            return;
//        }
//
//        // 6.扣减库存（乐观锁防超卖）
//        boolean success = seckillVoucherService.update()
//                .setSql("stock = stock - 1")
//                .eq("voucher_id", voucherOrder.getVoucherId())
//                .gt("stock", 0)
//                .update();
//        if (!success) {
//            log.error("库存不足");
//            return;
//        }
//        save(voucherOrder);
//
//    }
//
//
//    @Transactional
//    public Result createVoucherOrder2(Long voucherId) {
//        return Result.ok();
//    }
//}
