package com.atguigu.es

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType

/**
  * ES客户端
  */
object EsTest {
  def main(args: Array[String]): Unit = {
    //println(client)
    //put()
    //post()
    bulk()

    close()
  }

  /**
    *  批量写
    */
  def bulk(): Unit ={
    val bulkRequest: BulkRequest = new BulkRequest()
    val movies: List[Movie] = List[Movie](
      Movie("1002", "长津湖"),
      Movie("1003", "水门桥"),
      Movie("1004", "狙击手"),
      Movie("1005", "熊出没")
    )
    for (movie <- movies) {
      val indexRequest: IndexRequest = new IndexRequest("movie_test") // 指定索引
      val movieJson: String = JSON.toJSONString(movie, new SerializeConfig(true))
      indexRequest.source(movieJson ,XContentType.JSON)
      //幂等写指定id , 非幂等不指定id
      indexRequest.id(movie.id)
      //将indexRequest加入到bulk
      bulkRequest.add(indexRequest)
    }
    client.bulk(bulkRequest , RequestOptions.DEFAULT);
  }



  /**
    * 增 - 幂等 - 指定docid
    */
  def put(): Unit ={
    val indexRequest: IndexRequest = new IndexRequest()
    //指定索引
    indexRequest.index("movie_test")
    //指定doc
    val movie: Movie = Movie("1001","速度与激情1")
    val movieJson: String = JSON.toJSONString(movie, new SerializeConfig(true))
    indexRequest.source(movieJson,XContentType.JSON)
    //指定docid
    indexRequest.id("1001")

    client.index(indexRequest , RequestOptions.DEFAULT)
  }

  /**
    * 增 - 非幂等 - 不指定docid
    */
  def post(): Unit ={
    val indexRequest: IndexRequest = new IndexRequest()
    //指定索引
    indexRequest.index("movie_test")
    //指定doc
    val movie: Movie = Movie("1001","速度与激情1")
    val movieJson: String = JSON.toJSONString(movie, new SerializeConfig(true))
    indexRequest.source(movieJson,XContentType.JSON)
    client.index(indexRequest , RequestOptions.DEFAULT)
  }












  /**客户端对象*/
  var client : RestHighLevelClient = create()

  /**创建客户端对象*/
  def create(): RestHighLevelClient ={
    val restClientBuilder: RestClientBuilder =
        RestClient.builder(new HttpHost("hadoop102", 9200))
    val client: RestHighLevelClient = new RestHighLevelClient(restClientBuilder)
    client
  }

  /**关闭客户端对象*/
  def close(): Unit ={
    if(client!=null) client.close()
  }

}

case class Movie(id : String , movie_name : String )
