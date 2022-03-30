package com.atguigu.gmall.publisherrealtime.service.impl;

import com.atguigu.gmall.publisherrealtime.bean.NameValue;
import com.atguigu.gmall.publisherrealtime.mapper.PublisherMapper;
import com.atguigu.gmall.publisherrealtime.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * 业务层
 */
@Service
public class publisherServiceImpl implements PublisherService {

    @Autowired
    PublisherMapper publisherMapper ;

    /**
     * 日活分析业务处理
     */
    @Override
    public Map<String, Object> doDauRealtime(String td) {
        //业务处理
        Map<String, Object> dauResults  =  publisherMapper.searchDau(td) ;
        return dauResults;
    }

    /**
     * 交易分析业务处理
     */
    @Override
    public List<NameValue> doStatsByItem(String itemName, String date, String t) {
        //业务处理

        List<NameValue>  searcherResults =   publisherMapper.searchStatsByItem(itemName , date , t );

        return searcherResults;
    }
}
