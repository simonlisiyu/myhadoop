package com.lsy.myhadoop.geomesa.conf;

import com.alibaba.fastjson.JSONArray;
import org.geotools.data.*;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by lisiyu on 2019/7/18.
 */
public class GeomesaTest {
    private static final Logger logger = LoggerFactory.getLogger(GeomesaTest.class);


    public static void main(String[] args)  {
        String zk = "slave10.spark.com";
        String catalogTableName = "catlog_device";
        String featureName = "ftn_kakou_flow";

        List<String> attributes = new ArrayList<>();
        attributes.add("_ts");
        attributes.add("_id");

        Map<String, Object> params = new HashMap<String,Object>();
        params.put("hbase.zookeepers", zk); //【步骤1】 连接hbase集群的zookeeper地址, 或者通过在classpath中的hbase-site.xml配置，可覆盖此参数
        //或者通过params.put("hbase.config.paths", /home/configPath/); 指定hbase configuration resource files(逗号分隔)
        params.put("hbase.catalog", catalogTableName); //【步骤2】 "TRAFFIC:VEHICLE"
        params.put("hbase.coprocessor.url", "/hbase/lib/geomesa-hbase-distributed-runtime_2.11-2.0.0.jar");
        params.put("hbase.remote.filtering", true);
        params.put("geomesa.query.threads", 100);


        JSONArray array = new JSONArray();
        DataStore dataStore = null;

        try {
            dataStore = createDataStore(params); // 【step1】: 创建dataStore
            ensureSchema(dataStore, featureName); // 【step2】: 确认SimpleFeatureType 已存在

            Query query = new Query(featureName, ECQL.toFilter("_ts AFTER 2018-07-09T01:30:00Z"));
            query.setMaxFeatures(Integer.parseInt("10000"));
//            query.setStartIndex(Integer.parseInt("0"));  // not work
//            List<SimpleFeature> queryFeatureList = new ArrayList<>();
//            FeatureReader<SimpleFeatureType, SimpleFeature> reader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
//            int n = 0;
//            while(reader.hasNext()){
//                SimpleFeature feature=reader.next();
//                queryFeatureList.add(feature);
//                n++;
//            }
//            System.out.println(n);


//            Query query = getQuery(featureName); //【step3】: 生成Query对象
//            long start_plan = System.currentTimeMillis();
//            HBaseDataStore hBaseDataStore = (HBaseDataStore) dataStore;
//            hBaseDataStore.getQueryPlan(query, scala.Option.empty(), new ExplainPrintln(System.out)); // 【step4】可打印出queryPlan信息
//            long end_plan = System.currentTimeMillis();


//            FeatureReader<SimpleFeatureType, SimpleFeature> reader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);  //【step5】: 创建FeatureReader，程序启动后第一次执行此过程耗时最长，可达300ms+，需要查询元数据信息，如果前面有显示执行getQueryPlan的动作，时间消耗在getQueryPlan中体现
//            List<Map<String,Object>> resultList = queryFeatures(reader, attributes);
//            array = JSONArray.fromObject(resultList);

            FeatureReader<SimpleFeatureType, SimpleFeature> reader1 = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);  //【step5】: 创建FeatureReader，程序启动后第一次执行此过程耗时最长，可达300ms+，需要查询元数据信息，如果前面有显示执行getQueryPlan的动作，时间消耗在getQueryPlan中体现
            array = queryFeatures(reader1, attributes);

            System.out.println("result="+array.size());
            System.out.println("result="+array.getJSONObject(0));
//            logger.info("Query Plan takes " + (end_plan - start_plan) + " ms");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CQLException e) {
            e.printStackTrace();
        } finally {
            logger.info("Finished runQuery.");
        }

    }

    public static DataStore createDataStore(Map<String, Object> params) throws IOException {
        logger.info("Loading datastore");

        DataStore datastore = DataStoreFinder.getDataStore(params);
        if (datastore == null) {
            throw new RuntimeException("Could not create data store with provided parameters");
        }
        return datastore;
    }

    public static void ensureSchema(DataStore datastore, String featureName) throws IOException {
        SimpleFeatureType sft = datastore.getSchema(featureName);
        if (sft == null) {
            throw new IllegalStateException("Schema '" + featureName + "' does not exist. " +
                    "Please run the associated QuickStart to generate the test data.");
        }
    }

    public static Query getQuery(String featureName, String cql) throws CQLException {
        return new Query(featureName, ECQL.toFilter(cql));
    }

    /**
     * 备注：读取queryfeature中的结果
     * @param reader
     * */
    public static JSONArray queryFeatures(FeatureReader<SimpleFeatureType, SimpleFeature> reader, List keys) throws IOException {
        JSONArray resultList = new JSONArray();
        try {
            logger.info("Start Query");
            long start_time = System.currentTimeMillis();
            while (reader.hasNext()) {
                Map<String,Object> featureString = new LinkedHashMap<>();
                SimpleFeature feature = reader.next();
                for(Object key:keys){
                    featureString.put(key.toString(), feature.getAttribute(key.toString()).toString());
                }
                resultList.add(featureString);
            }
            long end_time = System.currentTimeMillis();
            int query_time = (int)(end_time - start_time);
            logger.info("Returned " + resultList.size() + " total features, total takes " + query_time + " ms");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            reader.close(); // 【step7】使用完需close reader
        }
        return resultList;
    }
}
