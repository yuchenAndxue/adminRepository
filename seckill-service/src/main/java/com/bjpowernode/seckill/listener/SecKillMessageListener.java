package com.bjpowernode.seckill.listener;

import com.alibaba.fastjson.JSONObject;
import com.bjpowernode.seckill.model.Orders;
import com.bjpowernode.seckill.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

/**
 * ClassName: SecKillMessageListener
 * Package: com.bjpowernode.seckill.listener
 * Description:
 *
 * @date: 2019/10/26 22:43
 * @author: wang
 */
@Component
public class SecKillMessageListener {

    @Autowired
    private OrderService orderService;

    @JmsListener(destination = "seckill")
    public void onMessage(String orderStr) {
        //System.out.println(orderStr);
        Orders orders = JSONObject.parseObject(orderStr, Orders.class);
        Integer result = orderService.addSecKillOrder(orders);
        if (result != 0) {
            System.out.println("添加订单失败! ");
        }
    }
}
