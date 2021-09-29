package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {
    //获取总数的抽象方法
    public Double selectOrderAmountTotal(String date);

    //获取分时数据的抽象方法
    public List<Map> selectOrderAmountHourMap(String date);
}
