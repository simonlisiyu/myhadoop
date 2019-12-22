package com.lsy.myhadoop.geomesa.service.executor.impl;

import com.google.common.base.Throwables;
import com.lsy.myhadoop.geomesa.service.constant.DataStoreConstant;
import com.lsy.myhadoop.geomesa.service.constant.ExceptionEnum;
import com.lsy.myhadoop.geomesa.service.entity.GeomesaContext;
import com.lsy.myhadoop.geomesa.service.executor.GeomesaExecutor;
import com.lsy.myhadoop.geomesa.service.utils.BusiVerify;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractGeomesaExecutor<K extends GeomesaContext, V> implements GeomesaExecutor<K, V> {

    @Override
    public V execute(K k) {
        DataStore dataStore = null;
        try {
            dataStore = createDataStore(k);

            ensureSchema(dataStore, k.getFeature());

            return queryResult(dataStore, k);
        } catch (Throwable e) {
             throw Throwables.propagate(e);
        } finally {
            if (dataStore != null) {
                dataStore.dispose();
            }
        }
    }

    private DataStore createDataStore(K k) throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put(DataStoreConstant.ZKS, k.getZookeepers());
        params.put(DataStoreConstant.CATALOG, k.getCatalog());
//        params.put("geomesa.query.threads", 20);
//        params.put(DataStoreConstant.CACHING, DataStoreConstant.TRUE);

        return createDataStore(params);
    }

    private DataStore createDataStore(Map<String, Object> params) throws IOException {
        DataStore datastore = DataStoreFinder.getDataStore(params);

        return BusiVerify.verifyNotNull(datastore, ExceptionEnum.PARAM_INVALID.getCode(), String.format("zookeepers=%s 参数错误，连接失败", params.get("zookeepers")));
    }

    private void ensureSchema(DataStore dataStore, String featureName) throws IOException {
        SimpleFeatureType sft = dataStore.getSchema(featureName);

        BusiVerify.verifyNotNull(sft, ExceptionEnum.PARAM_INVALID.getCode(), String.format("featureName=%s 不存在", featureName));
    }

    public abstract V queryResult(DataStore dataStore, K k) throws Exception;
}
