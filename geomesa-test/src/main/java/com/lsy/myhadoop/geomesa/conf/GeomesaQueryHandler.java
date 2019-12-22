package com.lsy.myhadoop.geomesa.conf;

import com.alibaba.fastjson.JSONArray;
import org.geotools.data.*;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.hbase.data.HBaseDataStore;
import org.locationtech.geomesa.index.utils.ExplainPrintln;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Created by lisiyu on 2019/6/5.
 */
public class GeomesaQueryHandler implements Callable<JSONArray> {
    private static final Logger logger = LoggerFactory.getLogger(GeomesaConf.class);

//    public final static String geomesaKey = "geomesa";
    private final Map<String, String> params;
    private String featureName;
    private String cql;
    private List attributes;

    public GeomesaQueryHandler(Context geomesaContext, String tableName, String cqlStr){
//        Context context = YamlUtils.getYamlContext(yamlFile, geomesaKey);
        GeomesaConf conf = new GeomesaConf(geomesaContext, tableName);
        params = conf.param;
        featureName = conf.featureTypeName;
        cql = cqlStr;
        attributes = conf.attributes;
    }



    @Override
    public JSONArray call() throws Exception {
        JSONArray array = new JSONArray();
        DataStore dataStore = null;

        try {
            dataStore = createDataStore(params); // 【step1】: 创建dataStore
            ensureSchema(dataStore, featureName); // 【step2】: 确认SimpleFeatureType 已存在
            Query query = getQuery(featureName, cql); //【step3】: 生成Query对象
            long start_plan = System.currentTimeMillis();
            HBaseDataStore hBaseDataStore = (HBaseDataStore) dataStore;
            hBaseDataStore.getQueryPlan(query, scala.Option.empty(), new ExplainPrintln(System.out)); // 【step4】可打印出queryPlan信息
            long end_plan = System.currentTimeMillis();
            FeatureReader<SimpleFeatureType, SimpleFeature> reader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);  //【step5】: 创建FeatureReader，程序启动后第一次执行此过程耗时最长，可达300ms+，需要查询元数据信息，如果前面有显示执行getQueryPlan的动作，时间消耗在getQueryPlan中体现
            array = queryFeatures(reader, attributes);
            logger.info("Query Plan takes " + (end_plan - start_plan) + " ms");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CQLException e) {
            e.printStackTrace();
        } finally {
            logger.info("Finished runQuery.");
        }

        return array;
    }

    /**
     * 备注：创建DataStore
     * @param params
     * */
    public DataStore createDataStore(Map<String, String> params) throws IOException {
        logger.info("Loading datastore");

        DataStore datastore = DataStoreFinder.getDataStore(params);
        if (datastore == null) {
            throw new RuntimeException("Could not create data store with provided parameters");
        }
        return datastore;
    }

    public void ensureSchema(DataStore datastore, String featureName) throws IOException {
        SimpleFeatureType sft = datastore.getSchema(featureName);
        if (sft == null) {
            throw new IllegalStateException("Schema '" + featureName + "' does not exist. " +
                    "Please run the associated QuickStart to generate the test data.");
        }
    }

    public Query getQuery(String featureName, String cql) throws CQLException {
        return new Query(featureName, ECQL.toFilter(cql));
    }

    /**
     * 备注：读取queryfeature中的结果
     * @param reader
     * */
    public JSONArray queryFeatures(FeatureReader<SimpleFeatureType, SimpleFeature> reader,List keys){
        JSONArray array = new JSONArray();
        try {
            logger.info("Start Query");
            long start_time = System.currentTimeMillis();
            while (reader.hasNext()) {
                Map<String,Object> featureString = new LinkedHashMap<>();
                SimpleFeature feature = reader.next();
                for(Object key:keys){
                    featureString.put(key.toString(), feature.getAttribute(key.toString()));
                }
                array.add(featureString);
            }
            reader.close(); // 【step7】使用完需close reader
            long end_time = System.currentTimeMillis();
            int query_time = (int)(end_time - start_time);
            logger.info("Returned " + array.size() + " total features, total takes " + query_time + " ms");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return array;
    }

}
