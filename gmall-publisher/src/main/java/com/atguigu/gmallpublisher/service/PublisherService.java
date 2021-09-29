package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {
    //获取日活总数数据
    public Integer getDauTotal(String date);

    //获取日活分时数据
    public Map getDauHourTotal(String date);

    //获取交易额总数数据
    public Double getGmvTotal(String date);

    //获取交易额分时数据
    public Map getGmvHourTotal(String date);
}
