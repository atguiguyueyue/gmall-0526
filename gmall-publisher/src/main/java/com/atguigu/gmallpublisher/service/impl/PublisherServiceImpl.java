package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.constants.GmallConstants;
import com.atguigu.gmallpublisher.bean.Options;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHourTotal(String date) {
        //1.获取Mapper层的数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //2.创建新的map集合用来存放结果数据
        HashMap<String, Long> result = new HashMap<>();

        //3.遍历list集合提取每个map
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }
        return result;
    }

    @Override
    public Double getGmvTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getGmvHourTotal(String date) {
        //1.获取mapper层查询的数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //2.创建map集合用来存放结果数据
        HashMap<String, Double> result = new HashMap<>();

        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }

    @Override
    public Map getSaleDetail(String date, Integer startpage, Integer size, String keyword) throws IOException {
        //1.从ES中查询数据
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤 匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //  性别聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        //  年龄聚合
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);

        // 行号= （页面-1） * 每页行数
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_SALE_DETAIL_QUERY).addType("_doc").build();

        SearchResult searchResult = jestClient.execute(search);
        //2.将查询出来的数据封装成规定的数据格式

        //TODO 1.total指的是命中的数据条数
        Long total = searchResult.getTotal();

        //TODO 2.获取明细数据
        ArrayList<Map> detail = new ArrayList<>();
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            detail.add( hit.source);
        }

        //TODO 3.获取年龄聚合组数据
        MetricAggregation aggregations = searchResult.getAggregations();
        TermsAggregation groupbyUserAge = aggregations.getTermsAggregation("groupby_user_age");
        List<TermsAggregation.Entry> buckets = groupbyUserAge.getBuckets();
        Long low20Count = 0L;
        Long up30Count = 0L;
        for (TermsAggregation.Entry bucket : buckets) {
            //获取年龄小于20岁的总人数
            if (Integer.parseInt(bucket.getKey())<20){
                low20Count = low20Count + bucket.getCount();
            }

            //获取年龄大于等于30岁的总数人数
            if (Integer.parseInt(bucket.getKey())>=30){
                up30Count = up30Count + bucket.getCount();
            }
        }

        //20岁以下年龄占比
        double low20Ratio = Math.round(low20Count * 1000D / total) / 10D;
        //30岁及30以上年龄占比
        double up30Ratio = Math.round(up30Count * 1000D / total) / 10D;
        //20岁到30岁年龄占比
        double up20AndLow30Ratio = Math.round((100D - low20Ratio - up30Ratio) * 10D) / 10D;

        Options low20Opt = new Options("20岁以下", low20Ratio);
        Options up20AndLow30Opt = new Options("20岁到30岁", up20AndLow30Ratio);
        Options up30Opt = new Options("30岁及30岁以上", up30Ratio);

        //创建存放年龄相关options对象的List集合
        ArrayList<Options> options = new ArrayList<>();
        options.add(low20Opt);
        options.add(up20AndLow30Opt);
        options.add(up30Opt);

        //创建年龄占比相关的Stat对象
        Stat ageStat = new Stat(options, "用户年龄占比");

        //TODO 4.获取性别聚合组数据
        TermsAggregation groupbyUserGender = aggregations.getTermsAggregation("groupby_user_gender");
        List<TermsAggregation.Entry> genderBuckets = groupbyUserGender.getBuckets();
        Long maleCount = 0L;
        for (TermsAggregation.Entry genderBucket : genderBuckets) {
            if ("M".equals(genderBucket.getKey())){
                maleCount += genderBucket.getCount();
            }
        }
        //获取男生占比
        double maleRatio = Math.round(maleCount * 1000D / total) / 10D;
        //女生占比
        double femaleRatio = Math.round((100D - maleRatio) * 10D) / 10D;

        Options maleOpt = new Options("男", maleRatio);
        Options femaleOpt = new Options("女", femaleRatio);

        //创建list集合用来存放性别相关的options对象
        ArrayList<Options> genderList = new ArrayList<>();
        genderList.add(maleOpt);
        genderList.add(femaleOpt);

        //创建性别相关的Stat对象
        Stat genderStat = new Stat(genderList, "用户性别占比");

        //创建list集合用来存放Stat对象
        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        //创建Map集合用来存放结果数据
        HashMap<String, Object> result = new HashMap<>();
        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", detail);

        return result;
    }
}
