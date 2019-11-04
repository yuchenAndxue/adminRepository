package com.bjpowernode.seckill.service.impl;

import com.alibaba.dubbo.config.annotation.Service;
import com.alibaba.fastjson.JSONObject;
import com.bjpowernode.seckill.commons.Constants;
import com.bjpowernode.seckill.commons.ReturnJsonObject;
import com.bjpowernode.seckill.mapper.GoodsMapper;
import com.bjpowernode.seckill.mapper.OrdersMapper;
import com.bjpowernode.seckill.model.Goods;
import com.bjpowernode.seckill.model.Orders;
import com.bjpowernode.seckill.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Date;

/**
 * ClassName: OrderServiceImpl
 * Package: com.bjpowernode.seckill.service.impl
 * Description:
 *
 * @date: 2019/10/24 21:50
 * @author: wang
 */
@Service(interfaceClass = OrderService.class)
@Component
public class OrderServiceImpl implements OrderService {

    @Autowired
    private OrdersMapper ordersMapper;

    @Autowired
    private GoodsMapper goodsMapper;

    @Autowired
    private RedisTemplate redisTemplate;

    private StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();

    @Override
    public Integer addSecKillOrder(Orders orders) {

        redisTemplate.setKeySerializer(stringRedisSerializer);
        redisTemplate.setValueSerializer(stringRedisSerializer);

        Goods goods = goodsMapper.selectByPrimaryKey(orders.getGoodsid());
        orders.setBuynum(1);
        orders.setCreatetime(new Date());
        orders.setStatus(1);
        orders.setBuyprice(goods.getPrice());
        orders.setOrdermoney(goods.getPrice().multiply(new BigDecimal(orders.getBuynum())));

        try {
            ordersMapper.insertSecKillOrder(orders);
            //将订单结果存入redis中通知用户进行支付
            redisTemplate.opsForValue().set(Constants.ORDERS_RESULT + orders.getGoodsid() + orders.getUid(), JSONObject.toJSONString(orders));
            //下单成功后需要移除redis中备份的订单
            redisTemplate.delete(Constants.ORDERS + orders.getId() + orders.getUid());
        } catch (Exception e) {
            e.printStackTrace();
            //进入catch表示插入数据时抛出异常原因是数据在表中已经存在当前消息已经写入数据库将消息直接从队列中移除
            //第四次防止用户重复购买这里可以100%保证用户不能重复购买同一件商品
            return 1;
        }

        return 0;
    }

    @Override
    public ReturnJsonObject getOrderResult(Integer uid, Integer goodsId) {

        redisTemplate.setKeySerializer(stringRedisSerializer);
        redisTemplate.setValueSerializer(stringRedisSerializer);
        ReturnJsonObject returnJsonObject = new ReturnJsonObject();

        String orderResultStr = (String) redisTemplate.opsForValue().get(Constants.ORDERS_RESULT + goodsId + uid);
        if (orderResultStr == null) {
            returnJsonObject.setCode(Constants.ERROR);
            returnJsonObject.setMessage("没有获取到订单数据");
            returnJsonObject.setResult("");
            return returnJsonObject;
        }

        returnJsonObject.setCode(Constants.OK);
        returnJsonObject.setMessage("成功获取到订单数据");
        returnJsonObject.setResult(JSONObject.parseObject(orderResultStr,Orders.class));

        return returnJsonObject;
    }
}
