package com.lsy.myhadoop.es.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lsy.myhadoop.es.commons.ElasticsearchHttpConst;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;

import java.io.IOException;

import static com.lsy.myhadoop.es.commons.ElasticsearchHttpConst.COMPARE_SYMBOL_ENUM.*;

/**
 * ES查询条件，json utils
 * Created by lisiyu on 2017/12/18.
 * 1. condition
 * 2. aggs
 * 3. bool
 * 4. query
 */
public class ElasticsearchDSLUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchDSLUtils.class);

    /**
     * create condition
     * 创建条件： 1:1, 1:n, n:n
     */
    // conditionName = "query_string";
    // key = "query";
    // value = "123";
    // {"query_string":{"analyze_wildcard":true,"query":"123"}}
    private static JSONObject createCondition(String conditionName, String key, Object value){
        JSONObject conditionJson = new JSONObject();
        JSONObject kvJson = new JSONObject();
        kvJson.put(key, value);
        if("query"==conditionName) kvJson.put("analyze_wildcard", true);
        conditionJson.put(conditionName, kvJson);
        return conditionJson;
    }
//    private static JSONObject createCondition(String conditionName, String key, Object[] value){
//        JSONObject conditionJson =s new JSONObject();
//        JSONObject kvJson = new JSONObject();
//        kvJson.put(key, value);
//        kvJson.put("analyze_wildcard", true);
//        conditionJson.put(conditionName, kvJson);
//        return conditionJson;
//    }
//    private static JSONObject createCondition(String conditionName, String key, JSONArray value){
//        JSONObject conditionJson = new JSONObject();
//        JSONObject kvJson = new JSONObject();
//        kvJson.put(key, value);
//        kvJson.put("analyze_wildcard", true);
//        conditionJson.put(conditionName, kvJson);
//        return conditionJson;
//    }
    private static JSONObject createCondition(String conditionName, String[] key, Object[] value){
        if(key.length != value.length){
            return null;
        }
        JSONObject conditionJson = new JSONObject();
        JSONObject kvJson = new JSONObject();
        for(int i=0;i<key.length;i++){
            kvJson.put(key[i], value[i]);
        }
        conditionJson.put(conditionName, kvJson);
        return conditionJson;
    }

    // term 等于
    public static JSONObject createTermCondition(String key, Object value){
        return createCondition("term", key, value);
    }
    // terms 等于（或关系）
    public static JSONObject createTermsCondition(String key, Object[] value){
        return createCondition("terms", key, value);
    }
    public static JSONObject createTermsCondition(String key, JSONArray value){
        return createCondition("terms", key, value);
    }
    // exists 存在（field字段）
    public static JSONObject createExistCondition(Object value){
        return createCondition("exists", "field", value);
    }
    // missing 不存在（field字段）
    public static JSONObject createMissingCondition(Object value){
        return createCondition("missing", "field", value);
    }
    // match 查询匹配（支持空格分隔的或关系）
    public static JSONObject createMatchCondition(String key, Object value){
        return createCondition("match", key, value);
    }
    // multi_match 多条件查询匹配
    public static JSONObject createMultiMatchCondition(String query, String[] fields){
        JSONObject conditionJson = new JSONObject();
        JSONObject kvJson = new JSONObject();
        kvJson.put("query", query);
        kvJson.put("fields", fields);
        conditionJson.put("multi_match", kvJson);
        return conditionJson;
    }
    // match_phrase 短语匹配（支持空格不分割的查询）
    public static JSONObject createMatchPhraseCondition(String key, Object value){
        return createCondition("match_phrase", key, value);
    }
    // prefix 前缀匹配
    public static JSONObject createPrefixCondition(String key, Object value){
        return createCondition("prefix", key, value);
    }
    // wildcard 模糊匹配，如 "postcode": "W?F*HW"
    public static JSONObject createWildcardCondition(String key, Object value){
        return createCondition("wildcard", key, value);
    }
    // regexp 正则匹配 如 "postcode": "W[0-9].+"
    public static JSONObject createRegexpCondition(String key, Object value){
        return createCondition("regexp", key, value);
    }
    // query_string 多功能查询语句
    public static JSONObject createQueryStringCondition(String[] fields, Object value){
        String[] keys = {"fields", "query"};
        Object[] values = {fields, value};
        return createCondition("query_string", keys, values);
    }
    public static JSONObject createQueryStringCondition(String[] keys, Object[] value){
        return createCondition("query_string", keys, value);
    }
    public static JSONObject createQueryStringCondition(String key, Object value){
        return createCondition("query_string", key, value);
    }

    // range 范围匹配
    public static JSONObject createRangeCondition(String key, String gtltgtelte, Object value){
        if(!gtltgtelte.equals(GREATER_THAN.getSymbol())
                && !gtltgtelte.equals(GREATER_THAN_OR_EQUAL.getSymbol())
                && !gtltgtelte.equals(LESS_THAN.getSymbol())
                && !gtltgtelte.equals(LESS_THAN_OR_EQUAL.getSymbol())){
            return null;
        }
        JSONObject conditionJson = createCondition(key, gtltgtelte, value);
        JSONObject rangeJson = new JSONObject();
        rangeJson.put("range", conditionJson);
        return rangeJson;
    }
    public static JSONObject createRangeCondition(String key, String[] gtltgteltes, Object[] value){
        for(String gtltgtelte : gtltgteltes){
            if(!gtltgtelte.equals(GREATER_THAN.getSymbol())
                    && !gtltgtelte.equals(GREATER_THAN_OR_EQUAL.getSymbol())
                    && !gtltgtelte.equals(LESS_THAN.getSymbol())
                    && !gtltgtelte.equals(LESS_THAN_OR_EQUAL.getSymbol())){
                return null;
            }
        }
        JSONObject conditionJson = createCondition(key, gtltgteltes, value);
        JSONObject rangeJson = new JSONObject();
        rangeJson.put("range", conditionJson);
        return rangeJson;
    }


    /**
     * create aggs
     * 创建条件：1.field agg, 2.distinct(field) agg
     */
    public static JSONObject createAggs(String aggsName, String aggField, long size, ElasticsearchHttpConst.ORDER_BY_SYMBOL_ENUM descOrAsc, JSONObject subAggsJson){
        JSONObject orderJson = new JSONObject();
        JSONObject termJson = new JSONObject();
        JSONObject aggJson = new JSONObject();
        JSONObject aggsJson = new JSONObject();
        orderJson.put("_term", descOrAsc);
        termJson.put("field", aggField);
        termJson.put("size", size);
        termJson.put("order", orderJson);
        aggJson.put("terms", termJson);
        aggJson.put("aggs", subAggsJson);
        aggsJson.put(aggsName, aggJson);
        return aggsJson;
    }
    // distinct field agg
    public static JSONObject createCardinalityAggs(String aggsName, String aggField, long precisionThreshold){
        JSONObject cardinalityJson = new JSONObject();
        JSONObject aggJson = new JSONObject();
        JSONObject aggsJson = new JSONObject();
        cardinalityJson.put("field", aggField);
        cardinalityJson.put("precision_threshold", precisionThreshold);
        aggJson.put("cardinality", cardinalityJson);
        aggsJson.put(aggsName, aggJson);
        return aggsJson;
    }


    /**
     * create bool
     * 多条件组合查询： must, must_not, should
     */
    private static JSONObject createBool(String judge, JSONObject... conditionJson){
        JSONObject boolJson = new JSONObject();
        boolJson.put(judge, conditionJson);
        return boolJson;
    }
    private static JSONObject addBool(JSONObject boolJson, String judge, JSONObject... conditionJson){
        boolJson.put(judge, conditionJson);
        return boolJson;
    }
    private static JSONObject addBool(JSONObject boolJson, String judge, JSONArray conditionJsonArray){
        boolJson.put(judge, conditionJsonArray);
        return boolJson;
    }
    public static JSONObject createMustNotBool(JSONObject conditionJson){
        return createBool("must_not", conditionJson);
    }
    public static JSONObject createMustBool(JSONObject conditionJson){
        return createBool("must", conditionJson);
    }
    public static JSONObject createShouldBool(JSONObject conditionJson){
        return createBool("should", conditionJson);
    }
    public static JSONObject addMustNotBool(JSONObject boolJson, JSONObject conditionJson){
        if(boolJson.containsKey("must_not")){
            JSONArray mustArray = boolJson.getJSONArray("must_not");
            mustArray.add(conditionJson);
            return addBool(boolJson, "must_not", mustArray);
        } else {
            return addBool(boolJson, "must_not", conditionJson);
        }
    }
    public static JSONObject addMustBool(JSONObject boolJson, JSONObject conditionJson){
        if(boolJson.containsKey("must")){
            JSONArray mustArray = boolJson.getJSONArray("must");
            mustArray.add(conditionJson);
            return addBool(boolJson, "must", mustArray);
        } else {
            return addBool(boolJson, "must", conditionJson);
        }
    }
    public static JSONObject addShouldBool(JSONObject boolJson, JSONObject conditionJson){
        if(boolJson.containsKey("should")){
            JSONArray mustArray = boolJson.getJSONArray("should");
            mustArray.add(conditionJson);
            return addBool(boolJson, "should", mustArray);
        } else {
            return addBool(boolJson, "should", conditionJson);
        }
    }


    /**
     * create query
     * 查询语句封装 query：filter
     */
    public static JSONObject createQueryStringQuery(JSONObject queryStringJson, long from, long size){
        JSONObject queryJson = new JSONObject();
        queryJson.put("from", from);
        queryJson.put("size", size);
        queryJson.put("query", queryStringJson);
        return queryJson;
    }

    public static JSONObject createFilterQuery(String queryParam, JSONObject conditionJson, long from, long size){
        JSONObject queryJson = new JSONObject();
        JSONObject paramJson = new JSONObject();
        paramJson.put(queryParam, conditionJson);
        queryJson.put("from", from);
        queryJson.put("size", size);
        JSONObject filterJson = new JSONObject();
        JSONObject filteredJson = new JSONObject();
        filterJson.put("filter", paramJson);
        filteredJson.put("filtered", filterJson);
        queryJson.put("query", filteredJson);
        return queryJson;
    }
    public static JSONObject createFilterQuery(String queryParam, JSONObject conditionJson, long from, long size, String sortKey, String descOrAsc){
        JSONObject queryJson = createFilterQuery(queryParam, conditionJson, from, size);
        JSONObject kvJson = new JSONObject();
        JSONObject sortJson = new JSONObject();
        kvJson.put("order", descOrAsc);
        sortJson.put(sortKey, kvJson);
        queryJson.put("sort", sortJson);
        return queryJson;
    }
    public static JSONObject createFilterQuery(String queryParam, JSONObject conditionJson, long from, long size, String[] sortKey, String[] descOrAsc){
        if(sortKey.length != descOrAsc.length){
            return null;
        }
        JSONArray sortArray = new JSONArray();
        for(int i=0;i<sortKey.length;i++){
            JSONObject kvJson = new JSONObject();
            JSONObject sortJson = new JSONObject();
            kvJson.put("order", descOrAsc[i]);
            sortJson.put(sortKey[i], kvJson);
            sortArray.add(sortJson);
        }
        JSONObject queryJson = createFilterQuery(queryParam, conditionJson, from, size);
        queryJson.put("sort", sortArray);
        return queryJson;
    }
    public static JSONObject createFilterBoolQuery(JSONObject boolJson, long from, long size){
        return createFilterQuery("bool", boolJson, from, size);
    }
    public static JSONObject createFilterBoolQuery(JSONObject boolJson, long from, long size, String sortKey, String descOrAsc){
        return createFilterQuery("bool", boolJson, from, size, sortKey, descOrAsc);
    }
    public static JSONObject createFilterBoolQuery(JSONObject boolJson, long from, long size, String[] sortKey, String[] descOrAsc){
        return createFilterQuery("bool", boolJson, from, size, sortKey, descOrAsc);
    }
    public static JSONObject createTotalQuery(JSONObject queryConditionJson, JSONObject boolJson, JSONObject aggsJson, long from, long size, String... sourceFields){
        JSONObject filterJson = new JSONObject();
        filterJson.put("bool", boolJson);

        JSONObject filteredJson = new JSONObject();
        filteredJson.put("query", queryConditionJson);
        filteredJson.put("filter", filterJson);

        JSONObject subQueryJson = new JSONObject();
        subQueryJson.put("filtered", filteredJson);
        JSONObject queryJson = new JSONObject();
        queryJson.put("_source", sourceFields);
        queryJson.put("query_string", subQueryJson);
        queryJson.put("aggs", aggsJson);
        queryJson.put("from", from);
        queryJson.put("size", size);
        return queryJson;
    }
    public static JSONObject acreateTotalQuery(JSONObject queryConditionJson, JSONObject aggsJson, long from, long size, String... sourceFields){
        JSONObject queryJson = new JSONObject();
        queryJson.put("_source", sourceFields);
        queryJson.put("query", queryConditionJson);
        queryJson.put("aggs", aggsJson);
        queryJson.put("from", from);
        queryJson.put("size", size);
        return queryJson;
    }

    /**
     * do search & get hits of response
     * @param esIndexUrl
     * @param query
     * @return {"data":[{},{},{}], "total":100}
     * @throws IOException
     */
    public static JSONObject getHitsSearchResponseJson(String esIndexUrl, String query) throws IOException {
        JSONObject jsonObject = getSearchResponseJson(esIndexUrl, query);
        JSONArray dataArray = jsonObject.getJSONObject("hits").getJSONArray("hits");
        int total = jsonObject.getJSONObject("hits").getInteger("total");
        JSONObject returnJson = new JSONObject();
        returnJson.put("data", dataArray);
        returnJson.put("total", total);
        return returnJson;
    }
    private static JSONObject getSearchResponseJson(String esIndexUrl, String query) throws IOException {
        LOG.debug("query="+query);
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        String str = HttpUtils.doPost(esIndexUrl, query.toString(), headers);
        JSONObject jsonObject = JSONObject.parseObject(str);
        return jsonObject;
    }

    /**
     * do search & get hits of response
     * @param esIndexUrl
     * @param query
     * @return {"data":{"1.xx":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[{"key":"mongo","doc_count":39,}]}}, "total":100}
     * @throws IOException
     */
    public static JSONObject getAggsSearchResponseJson(String esIndexUrl, String query) throws IOException {
        JSONObject jsonObject = getSearchResponseJson(esIndexUrl, query);
        JSONObject aggsJson = jsonObject.getJSONObject("aggregations");
        int total = jsonObject.getJSONObject("hits").getInteger("total");
        JSONObject returnJson = new JSONObject();
        returnJson.put("data", aggsJson);
        returnJson.put("total", total);
        return returnJson;
    }

}
