package com.atguigu.gmall.publisherrealtime.mapper.impl;

import com.atguigu.gmall.publisherrealtime.bean.NameValue;
import com.atguigu.gmall.publisherrealtime.mapper.PublisherMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Repository
public class PublisherMapperImpl  implements PublisherMapper {


    @Autowired
    RestHighLevelClient esClient ;

    private String indexNamePrefix = "gmall_dau_info_1018_";

    @Override
    public List<NameValue> searchStatsByItem(String itemName, String date, String t) {
        return null;
    }




    @Override
    public Map<String, Object> searchDau(String td) {
        Map<String,Object> dauResults  = new HashMap<>();
        //日活总数
        Long dauTotal = searchDauTotal(td);
        dauResults.put("dauTotal",dauTotal) ;

        //今日分时明细
        Map<String, Long> dauTd = searchDauHr(td);
        dauResults.put("dauTd", dauTd);

        //昨日分时明细
        //计算昨日
        LocalDate tdLd = LocalDate.parse(td);
        LocalDate ydLd = tdLd.minusDays(1);
        Map<String, Long> dauYd = searchDauHr(ydLd.toString());
        dauResults.put("dauYd", dauYd);

        return dauResults;
    }

    public  Map<String,Long> searchDauHr(String td ){
        HashMap<String, Long> dauHr = new HashMap<>();

        String indexName  = indexNamePrefix + td ;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //不要明细
        searchSourceBuilder.size(0);
        //聚合
        TermsAggregationBuilder termsAggregationBuilder =
                AggregationBuilders.terms("groupbyhr").field("hr").size(24);
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms parsedTerms = aggregations.get("groupbyhr");
            List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                String hr = bucket.getKeyAsString();
                long hrTotal = bucket.getDocCount();

                dauHr.put(hr, hrTotal);
            }

            return dauHr ;

        } catch (ElasticsearchStatusException ese){
            if(ese.status() == RestStatus.NOT_FOUND){
                log.warn( indexName +" 不存在......");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败....");
        }

        return dauHr ;
    }


    public Long searchDauTotal(String td ){
        String indexName = indexNamePrefix + td ;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //不要明细
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            long dauTotals = searchResponse.getHits().getTotalHits().value;
            return dauTotals ;
        }catch (ElasticsearchStatusException ese){
            if(ese.status() == RestStatus.NOT_FOUND){
                log.warn( indexName +" 不存在......");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败....");
        }
        return 0L;
    }

    public static void main(String[] args) {
        String td = "2022-03-30" ;
        LocalDate tdLd = LocalDate.parse(td);
        LocalDate ydLd = tdLd.minusDays(1);
        System.out.println(ydLd.toString());
    }
}
