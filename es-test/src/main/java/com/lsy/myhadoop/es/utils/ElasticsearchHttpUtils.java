package com.lsy.myhadoop.es.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lsy.myhadoop.es.commons.ElasticsearchHttpConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;

import java.io.IOException;

import static com.lsy.myhadoop.es.commons.ElasticsearchHttpConst.*;
import static com.lsy.myhadoop.es.commons.ElasticsearchHttpConst.COMPARE_SYMBOL_ENUM.*;

/**
 * Created by lisiyu on 2019/6/21.
 */
public class ElasticsearchHttpUtils {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchHttpUtils.class);
//    private final static String ip = "10.179.40.207";
//    private final static String port = "9200";
//    private final static String GREATER_THAN_OR_EQUAL = "gte";
//    private final static String LESS_THAN_OR_EQUAL = "lte";
//    private final static String ES_QUERY = "query";
//    private final static String ES_SEARCH = "_search";
//    private final static String ES_SOURCE = "_source";
//    private final static String ES_HITS = "hits";
//    private final static String SYMBOL_COMMA = ",";


    public static JSONObject queryString(ElasticsearchHttpConfig config, String index, String param,
                                         long from, long size, String... sourceFields) throws IOException {
        logger.info("enter ElasticsearchHttpUtils.queryString().");
        JSONObject queryStringConditionJson = ElasticsearchDSLUtils.createQueryStringCondition(ES_QUERY, param);
//        JSONObject updateRangeJson = ElasticsearchDSLUtils.createRangeCondition(timeKey,
//                (GREATER_THAN_OR_EQUAL.getSymbol()+SYMBOL_COMMA+LESS_THAN_OR_EQUAL.getSymbol()).split(SYMBOL_COMMA),
//                (startTs+SYMBOL_COMMA+endTs).split(SYMBOL_COMMA));
////        System.out.println("updateRangeJson="+updateRangeJson);
//        JSONObject boolJson = ElasticsearchDSLUtils.createMustBool(updateRangeJson);
        JSONObject queryJson = ElasticsearchDSLUtils.createTotalQuery(
                queryStringConditionJson,
                new JSONObject(), new JSONObject(),
                from, size, sourceFields);

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        String url = "http://"+config.getIp()+":"+config.getPort()+"/"+index+"/"+ES_SEARCH;
        logger.info("url={}, queryJson={}",url, queryJson);
        String str = HttpUtils.doPost(url, queryJson.toString(), headers);
        JSONObject jsonObject = JSONObject.parseObject(str, JSONObject.class);
        logger.info("es resturn jsonObject={}", jsonObject);
        return jsonObject;

    }

    /**
     *
     * @param index test222
     * @param param ddd
     * @param timeKey   _updatetime
     * @param startTs   1560392323000
     * @param endTs     1560392323999
     * @param from      0
     * @param size      10
     * @return
     * @throws IOException
     */
    public static JSONObject queryString(ElasticsearchHttpConfig config, String index, String param,
                                        String timeKey, long startTs, long endTs,
                                        long from, long size, String... sourceFields) throws IOException {
        logger.info("enter ElasticsearchHttpUtils.queryString().");
        JSONObject queryStringConditionJson = ElasticsearchDSLUtils.createQueryStringCondition(ES_QUERY, param);
        JSONObject updateRangeJson = ElasticsearchDSLUtils.createRangeCondition(timeKey,
                (GREATER_THAN_OR_EQUAL.getSymbol()+SYMBOL_COMMA+LESS_THAN_OR_EQUAL.getSymbol()).split(SYMBOL_COMMA),
                (startTs+SYMBOL_COMMA+endTs).split(SYMBOL_COMMA));
//        System.out.println("updateRangeJson="+updateRangeJson);
        JSONObject boolJson = ElasticsearchDSLUtils.createMustBool(updateRangeJson);
        JSONObject queryJson = ElasticsearchDSLUtils.createTotalQuery(
                queryStringConditionJson,
                boolJson,
                new JSONObject(),
                from, size, sourceFields);

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        String url = "http://"+config.getIp()+":"+config.getPort()+"/"+index+"/"+ES_SEARCH;
        logger.info("url={}, queryJson={}",url, queryJson);
        String str = HttpUtils.doPost(url, queryJson.toString(), headers);
        JSONObject jsonObject = JSONObject.parseObject(str, JSONObject.class);
        logger.info("es resturn jsonObject={}", jsonObject);
        return jsonObject;

//        JSONArray dataArray = new JSONArray();
//        JSONArray array = jsonObject.getJSONObject(ES_HITS).getJSONArray(ES_HITS);
//        for(int i=0; i<array.size(); i++){
//            JSONObject json = array.getJSONObject(i);
//            JSONObject data = json.getJSONObject(ES_SOURCE);
//            dataArray.add(data);
//        }
//
//        logger.info("success ElasticsearchHttpUtils.queryString(). data={}", dataArray);
//        return dataArray;
    }

    public static JSONObject queryTerm(ElasticsearchHttpConfig config, String index, String termKey, String termValue,
                                        String timeKey, long startTs, long endTs,
                                        long from, long size) throws IOException {
        logger.info("enter ElasticsearchHttpUtils.queryTerm().");
        JSONObject termJson = ElasticsearchDSLUtils.createTermCondition(termKey, termValue);
        JSONObject updateRangeGteJson = ElasticsearchDSLUtils.createRangeCondition(timeKey, "gte", startTs);
        JSONObject updateRangeLteJson = ElasticsearchDSLUtils.createRangeCondition(timeKey, "lte", endTs);

        JSONObject boolJson = ElasticsearchDSLUtils.createMustBool(updateRangeGteJson);
        boolJson = ElasticsearchDSLUtils.addMustBool(boolJson, updateRangeLteJson);
        boolJson = ElasticsearchDSLUtils.addMustBool(boolJson, termJson);

        JSONObject queryJson = ElasticsearchDSLUtils.createFilterBoolQuery(boolJson, from, size);

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        String url = "http://"+config.getIp()+":"+config.getPort()+"/"+index+"/"+ES_SEARCH;
        logger.info("url={}, queryJson={}", url, queryJson);
        String str = HttpUtils.doPost(url, queryJson.toString(), headers);
        JSONObject jsonObject = JSONObject.parseObject(str, JSONObject.class);
        logger.info("jsonObject={}", jsonObject);
        return jsonObject;

//        JSONArray dataArray = new JSONArray();
//        JSONArray array = jsonObject.getJSONObject(ES_HITS).getJSONArray(ES_HITS);
//        for(int i=0; i<array.size(); i++){
//            JSONObject json = array.getJSONObject(i);
//            JSONObject data = json.getJSONObject(ES_SOURCE);
//            dataArray.add(data);
//        }
//
//        logger.info("success ElasticsearchHttpUtils.queryTerm(). data={}",dataArray);
//        return dataArray;
    }

    public static JSONArray getSourceArrayFromResultJson(JSONObject resultJson, String id){
        JSONArray dataArray = new JSONArray();
        JSONArray array = resultJson.getJSONObject(ES_HITS).getJSONArray(ES_HITS);
        for(int i=0; i<array.size(); i++){
            JSONObject json = array.getJSONObject(i);
            JSONObject data = json.getJSONObject(ES_SOURCE);
            String _type = json.getString(ES_TYPE);
            data.put("type", _type);
            data.put("obj_id", data.getString(id));
            dataArray.add(data);
        }
        return dataArray;
    }
}
