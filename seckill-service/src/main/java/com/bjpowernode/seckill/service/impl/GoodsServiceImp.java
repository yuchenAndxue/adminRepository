package com.bjpowernode.seckill.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.dubbo.config.annotation.Service;
import com.bjpowernode.seckill.commons.Constants;
import com.bjpowernode.seckill.commons.ReturnJsonObject;
import com.bjpowernode.seckill.mapper.GoodsMapper;
import com.bjpowernode.seckill.mapper.OrdersMapper;
import com.bjpowernode.seckill.model.Goods;
import com.bjpowernode.seckill.model.Orders;
import com.bjpowernode.seckill.service.GoodsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: GoodsServiceImp
 * Package: com.bjpowernode.seckill.service.impl
 * Description:
 *
 * @date: 2019/10/24 21:49
 * @author: wang
 */
@Service(interfaceClass = GoodsService.class)
@Component
public class GoodsServiceImp implements GoodsService {

    @Autowired
    private GoodsMapper goodsMapper;

    @Autowired
    private OrdersMapper ordersMapper;

    @Autowired
    private RedisTemplate redisTemplate;

    private StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();

    @Autowired
    private JmsTemplate jmsTemplate;

    @Override
    public List<Goods> getGoodsAllList() {

        List<Goods> goodsList = goodsMapper.selectAllGoodsList();

        return goodsList;
    }

    @Override
    public Goods getGoodsInfoById(Integer id) {
        return goodsMapper.selectByPrimaryKey(id);
    }

    /**
     * TODO
     * @param id
     * @return
     */
    public String getGoodsInfoById1(Integer id) {
        redisTemplate.setKeySerializer(stringRedisSerializer);
        redisTemplate.setValueSerializer(stringRedisSerializer);
        Object obj = redisTemplate.opsForValue().get(String.valueOf(id));
        String strGoods = null;
        if (obj == null) {
            synchronized (this) {
                obj = redisTemplate.opsForValue().get(String.valueOf(id));
                if (obj == null) {
                    Goods goods = goodsMapper.selectByPrimaryKey(id);
                    strGoods = goods.toString();
                    redisTemplate.opsForValue().set(String.valueOf(id), strGoods, 15, TimeUnit.MINUTES);
                }
            }
        }

        return strGoods;
    }


    /**
     * 秒杀业务逻辑方法
     * @param uid  用户id
     * @param goodsId  商品id
     * @param randomName  商品随机名
     * @return  秒杀业务逻辑处理结果
     */
    @Override
    public ReturnJsonObject secKill(Integer uid, Integer goodsId, String randomName) {

        ReturnJsonObject returnJsonObject = new ReturnJsonObject();
        redisTemplate.setKeySerializer(stringRedisSerializer);
        redisTemplate.setValueSerializer(stringRedisSerializer);
        String strStore = (String) redisTemplate.opsForValue().get(Constants.GOODS_STORE + randomName);

        //如果查询到strStore为null, 则可能表示redis中不存在或可能没有开始秒杀或是已结束
        if (strStore == null) {
            returnJsonObject.setCode(Constants.ERROR);
            returnJsonObject.setMessage("对不起! 您所购买的商品异常!");
            returnJsonObject.setResult("");
            return returnJsonObject;
        }

        Integer store = Integer.valueOf(strStore);
        //判断商品库存(第一次控制商品超卖,不能100%控制用户请求)
        if (store <= 0) {
            returnJsonObject.setCode(Constants.ERROR);
            returnJsonObject.setMessage("对不起! 您所购买的商品已被抢光!");
            returnJsonObject.setResult("");
            return returnJsonObject;
        }

        //使用商品随机名+用户id+固定前缀来控制当前用户只能购买一次这个商品
        String purchaseLimits = (String) redisTemplate.opsForValue().get(Constants.PURCHASE_LIMITS + randomName + uid);
        //第二次控制用户重复个购买（重复提交请求），不能100%控制用户重复购买
        if (purchaseLimits != null) {
            returnJsonObject.setCode(Constants.ERROR);
            returnJsonObject.setMessage("对不起！您已经购买了这个商品不能重复下单！");
            returnJsonObject.setResult("");
            return returnJsonObject;
        }

        //限流自动增加1 然后返回当前记录的值 这个值可能大于1000或小于1000
        //我们设定的限流人数是1000，这可以是一个固定的值也可以使用商品数量*某个固定倍数例如 1000*100
        Long currentLimiting = redisTemplate.opsForValue().increment(Constants.CURRENT_LIMITING);
        if (currentLimiting > 1000) {
            returnJsonObject.setCode(Constants.ERROR);
            returnJsonObject.setMessage("对不起！服务器繁忙请稍后再试！");
            returnJsonObject.setResult("");
            //减少一个限流人数将刚刚的自动增加回滚
            redisTemplate.opsForValue().decrement(Constants.CURRENT_LIMITING);
            return returnJsonObject;
        }

        //定义订单对象 设置对象中的用户id以及商品id
        Orders orders = new Orders();
        orders.setUid(uid);
        orders.setGoodsid(goodsId);
        //执行秒杀业务实现减少库存
        Object obj = redisTemplate.execute(new SessionCallback(){
            @Override
            public Object execute(RedisOperations redisOperations) throws DataAccessException {
                List<String> list = new ArrayList<String>();
                list.add(Constants.GOODS_STORE + randomName);
                list.add(Constants.PURCHASE_LIMITS + randomName + uid);

                //添加key的监控(监控商品的库存以及用户限购)，监控key之后同时会锁定数据
                redisOperations.watch(list);
                //获取商品的库存
                Integer store = Integer.valueOf(redisOperations.opsForValue().get(Constants.GOODS_STORE + randomName)+ "");
                if (store <= 0) {
                    returnJsonObject.setCode(Constants.ERROR);
                    returnJsonObject.setMessage("对不起! 您所购买的商品已被抢光!");
                    returnJsonObject.setResult("");
                    return returnJsonObject;
                }
                String purchaseLimits = (String) redisTemplate.opsForValue().get(Constants.PURCHASE_LIMITS + randomName + uid);
                if (purchaseLimits != null) {
                    returnJsonObject.setCode(Constants.ERROR);
                    returnJsonObject.setMessage("对不起！您已经购买了这个商品不能重复下单！");
                    returnJsonObject.setResult("");
                    return returnJsonObject;
                }

                //开启Redis事务
                redisOperations.multi();
                //添加用户限购记录
                redisOperations.opsForValue().set(Constants.PURCHASE_LIMITS + randomName + uid, "1");
                //Redis中减少库存
                redisOperations.opsForValue().decrement(Constants.GOODS_STORE + randomName);
                //将订单记录存入Redis中防止调单的行为，然后通过定时任务不停的扫描Redis中的数据确定订单是否已经存入消息队列
                //或是否写入数据库，如果确认订单调单了那么把订单记录写入队列即可
                redisOperations.opsForValue().set(Constants.ORDERS + goodsId + uid, JSONObject.toJSONString(orders));
                //提交事务 返回List类型,
                return redisOperations.exec();
            }
        });

        //
        if (obj instanceof ReturnJsonObject) {
            //减少一个限流人数
            redisTemplate.opsForValue().decrement(Constants.CURRENT_LIMITING);
            //直接返回响应表示秒杀失败
            return (ReturnJsonObject) obj;
        }

        List resultList = (List) obj;
        //进入if表示Redis事务提交失败（放弃了事务提交）
        //事务提交失败有2种情况 1.Redis命令错误 2.监控的key被其他线程修改
        //进入if的具体原因是key被其他线程修改了但是我们不能判断出来到底是因为库存被修改了还是限购了
        if (resultList.isEmpty()) {
            //这里可以直接为用户返回响应提示秒杀失败或进行递归调用
            //减少一个限流人数
            redisTemplate.opsForValue().decrement(Constants.CURRENT_LIMITING);
            //递归调用自动进入一轮抢购
            return secKill(uid, goodsId, randomName);
        }

        //完成数据库下单
        //将订单数据存入消息队列然后异步完成下单
        jmsTemplate.send(new MessageCreator() {
            @Override
            public Message createMessage(Session session) throws JMSException {
                return session.createTextMessage(JSONObject.toJSONString(orders));
            }
        });

        //消息发送到队列以后，用户立即返回释放了Tomcat的连接那么限流人数也应该减少1个
        redisTemplate.opsForValue().decrement(Constants.CURRENT_LIMITING);
        returnJsonObject.setCode(Constants.OK);
        returnJsonObject.setMessage("下单成功");
        returnJsonObject.setResult(orders);

        return returnJsonObject;
    }

}
