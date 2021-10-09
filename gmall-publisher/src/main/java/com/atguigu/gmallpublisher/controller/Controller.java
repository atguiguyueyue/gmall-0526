package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import com.google.gson.JsonObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class Controller {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDapTotal(@RequestParam("date") String date) {
        //1.获取日活总数数据
        Integer dauTotal = publisherService.getDauTotal(date);

        //获取交易额总数数据
        Double gmvTotal = publisherService.getGmvTotal(date);

        //2.创建list集合用来存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //3.创建map集合用来分别存放新增日活数据和新增设备数据
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        HashMap<String, Object> devMap = new HashMap<>();
        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);

        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", gmvTotal);

        //4.将封装好的map集合存放到list集合中
        result.add(dauMap);
        result.add(devMap);
        result.add(gmvMap);

        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauHourTotal(@RequestParam("id") String id,
                                  @RequestParam("date") String date
    ) {
        //1.获取Service层的数据
        //获取昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        Map todayMap = null;
        Map yesterdayMap = null;
        if ("order_amount".equals(id)) {
            todayMap = publisherService.getGmvHourTotal(date);
            yesterdayMap = publisherService.getGmvHourTotal(yesterday);

        } else if ("dau".equals(id)) {
            //获取当天的数据
            todayMap = publisherService.getDauHourTotal(date);
            //获取昨天的数据
            yesterdayMap = publisherService.getDauHourTotal(yesterday);
        }

        //2.创建map集合用来存放结果数据
        HashMap<String, Map> result = new HashMap<>();
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);


        return JSONObject.toJSONString(result);
    }

    @RequestMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,
                                @RequestParam("startpage") Integer startpage,
                                @RequestParam("size") Integer size,
                                @RequestParam("keyword") String keyword
                                ) throws IOException {

        return JSONObject.toJSONString(publisherService.getSaleDetail(date, startpage, size, keyword));
    }
}
