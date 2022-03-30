package com.atguigu.gmall.publisherrealtime.mapper;

import com.atguigu.gmall.publisherrealtime.bean.NameValue;

import java.util.List;
import java.util.Map;

public interface PublisherMapper {
    Map<String, Object> searchDau(String td);

    List<NameValue> searchStatsByItem(String itemName, String date, String t);
}
