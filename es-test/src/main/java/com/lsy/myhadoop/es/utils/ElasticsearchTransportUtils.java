package com.lsy.myhadoop.es.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;


/**
 * Created by lisiyu on 2017/8/17.
 */
public class ElasticsearchTransportUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchTransportUtils.class);

    /**
     * 通过id拿到结果
     * @param client
     * @param index
     * @param type
     * @param id
     * @return
     */
    public static JSONObject getById(Client client, String index, String type, String id){
        LOG.info("enter ElasticsearchUtils.getById().");
        GetResponse response = client.prepareGet(index, type, id).get();
        JSONObject jsonObject = null;
        try {
            jsonObject = JSONObject.parseObject(response.getSourceAsString(), JSONObject.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("success ElasticsearchUtils.getById(). jsonObject="+jsonObject);
        return jsonObject;
    }

    /**
     * 根据查询条件查询结果
     * @param client
     * @param index
     * @param type
     * @param datetimeKey
     * @param from
     * @param to
     * @param key
     * @param value
     * @param position
     * @param size
     * @return
     */
    public static JSONArray queryByKv(Client client, String index, String type,
                                      String datetimeKey, long from, long to, String key, String value,
                                      int position, int size) throws IOException {
        LOG.info("enter ElasticsearchUtils.queryByKv().");
        QueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery(datetimeKey).to(to).from(from))
                .must(QueryBuilders.termQuery(key, value));
        SearchRequestBuilder builder = client
                .prepareSearch(index).setTypes(type)
                .setQuery(query)
                .addSort(datetimeKey, SortOrder.DESC)
                .setFrom(position).setSize(size).setExplain(true);
        SearchResponse response  = builder.execute().actionGet();
        SearchHits searchHit = response.getHits();
//        long totalCount = response.getHits().getTotalHits();

        JSONArray resultArray = new JSONArray();
        for (int i = 0; i < searchHit.getHits().length; i++) {
            String json = searchHit.getHits()[i].getSourceAsString();
            JSONObject jsonObject = JSONObject.parseObject(json, JSONObject.class);
            resultArray.add(jsonObject);
        }
        LOG.info("success ElasticsearchUtils.queryByKv(). resultArray="+resultArray);
        return resultArray;
    }

    public static JSONArray queryByKvs(Client client, String index, String type,
                                      String rangeKey, String gte, String lte, Map<String, String> kvs,
                                      int position, int size) throws IOException {
        LOG.info("enter ElasticsearchUtils.queryByKv().");
        JSONObject queryJson = new JSONObject();
        JSONObject boolJson = new JSONObject();
        JSONArray mustArray = new JSONArray();
        JSONObject glteJson = new JSONObject();
        JSONObject rangeJson = new JSONObject();
        JSONObject conditionJson = new JSONObject();
        glteJson.put("gte", gte);
        glteJson.put("lte", lte);
        rangeJson.put(rangeKey, glteJson);
        conditionJson.put("range", rangeJson);
        mustArray.add(conditionJson);

        for(Map.Entry<String, String> entry : kvs.entrySet()){
            JSONObject termJson = new JSONObject();
            conditionJson = new JSONObject();
            termJson.put(entry.getKey(), entry.getValue());
            conditionJson.put("term", termJson);
            mustArray.add(conditionJson);
        }
        boolJson.put("must", mustArray);
        queryJson.put("bool", boolJson);

        LOG.info("queryJson="+queryJson);
        QueryBuilder match = QueryBuilders.multiMatchQuery(queryJson);

        SearchRequestBuilder builder = client
                .prepareSearch(index).setTypes(type)
                .setQuery(match)
                .addSort(rangeKey, SortOrder.DESC)
                .setFrom(position).setSize(size).setExplain(true);
        SearchResponse response  = builder.execute().actionGet();
        SearchHits searchHit = response.getHits();
//        long totalCount = response.getHits().getTotalHits();

        JSONArray resultArray = new JSONArray();
        for (int i = 0; i < searchHit.getHits().length; i++) {
            String json = searchHit.getHits()[i].getSourceAsString();
            JSONObject jsonObject = JSONObject.parseObject(json, JSONObject.class);
            resultArray.add(jsonObject);
        }
        LOG.info("success ElasticsearchUtils.queryByKv(). resultArray="+resultArray);
        return resultArray;
    }

    /**
     * 读取 es search response json data
     * @param searchResponseJson
     * @return
     */
    public static JSONObject getHitsFromSearchResponseJson(JSONObject searchResponseJson){
        JSONArray hitsArray = searchResponseJson.getJSONObject("hits").getJSONArray("hits");
        int total = searchResponseJson.getJSONObject("hits").getInteger("total");
        JSONObject returnJson = new JSONObject();
        returnJson.put("data", hitsArray);
        returnJson.put("total", total);
        return returnJson;
    }
}
