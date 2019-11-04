package com.bjpowernode.seckill.scheduling;

import com.bjpowernode.seckill.commons.Constants;
import com.bjpowernode.seckill.mapper.GoodsMapper;
import com.bjpowernode.seckill.model.Goods;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * ClassName: SecKillScheduling
 * Package: com.bjpowernode.seckill.scheduling
 * Description:
 *          定时任务
 * @date: 2019/10/26 18:40
 * @author: wang
 */
@EnableScheduling
@Component
public class SecKillScheduling {

    @Autowired
    private GoodsMapper goodsMapper;

    @Autowired
    private RedisTemplate redisTemplate;

    private StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();

    @Scheduled(cron = "0/5 * * * * *")
    public void initRedisGoodsStore(){
        redisTemplate.setKeySerializer(stringRedisSerializer);
        redisTemplate.setValueSerializer(stringRedisSerializer);

        //获取需要添加到Redis中的所有商品数据，我们这里是获取数据库中所有数据
        //注意：实际工作中不能获取所有数据，我们只需要即将参与秒杀的商品
        List<Goods> goodsList = goodsMapper.selectAllGoodsList();
        for (Goods goods : goodsList) {
            //使用商品的随意名+固定前缀作为key 商品的的库存作为value初始化商品数据到Redis
            redisTemplate.opsForValue().setIfAbsent(Constants.GOODS_STORE + goods.getRandomname(),goods.getStore() + "");
        }
    }
}
