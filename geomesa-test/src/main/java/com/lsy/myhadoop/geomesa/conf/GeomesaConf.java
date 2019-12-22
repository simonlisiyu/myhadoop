package com.lsy.myhadoop.geomesa.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lisiyu on 2019/6/4.
 */
public class GeomesaConf {
    private static final Logger logger = LoggerFactory.getLogger(GeomesaConf.class);

    public Map<String,String> param = new HashMap<>();
//    public String catalog;
    public String featureTypeName;
    public List attributes;
    public Map<String,String> attributesIndex;

    public GeomesaConf(Context geomesaContext, String tableName) throws NullPointerException{
//        catalog = geomesaContext.get(tableName + ".catlog.name", String.class);
        featureTypeName = geomesaContext.get(tableName + ".feature.name", String.class);
        attributes = geomesaContext.get(tableName + ".attributes", List.class);
        attributesIndex  = new HashMap<>();
        Map<String,String> linkMap = geomesaContext.get(tableName + ".index.attributes", Map.class);
        for (Map.Entry entry : linkMap.entrySet()) {
            attributesIndex.put(entry.getKey().toString(), entry.getValue().toString());
        }

        param.put("hbase.zookeepers", geomesaContext.get("zk", String.class));
        param.put("hbase.coprocessor.url", geomesaContext.get("coprocessor_url", String.class));
        param.put("hbase.catalog", geomesaContext.get(tableName + ".catlog.name", String.class));
    }

    public static void main(String[] args) {
        Context context = YamlUtils.getYamlContext("dcp-base-hbase/src/main/resources/jn-geomesa.yaml", "geomesa");
        GeomesaConf conf = new GeomesaConf(context, "db_police");
        for (Map.Entry entry : conf.param.entrySet()) {
            System.out.println(entry.getKey().toString() + ":" +entry.getValue().toString());
        }

        System.out.println(conf.featureTypeName);
        System.out.println(conf.attributes);
        System.out.println(conf.attributesIndex);
        System.out.println(conf.attributesIndex.get("policeid"));
    }
}
