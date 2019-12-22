package com.lsy.myhadoop.geomesa.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;

/**
 * Created by lisiyu on 2017/6/2.
 */
public class YamlUtils {
    private static final Logger LOG = LoggerFactory.getLogger(YamlUtils.class);
    public static Map config = new HashMap();
    private static Yaml yaml = new Yaml();


    /**
     * 从yaml配置文件中读取conf，更新静态变量
     */
    public static Context getYamlContext(String yamlFileName, String pKey) {
        Map map = YamlUtils.getYamlConfig(yamlFileName, new HashMap());
        return new Context((Map)map.get(pKey));
    }

    public static void main(String[] args) {
        LOG.info("12312");
        Context ConstContext = getYamlContext("dcp-knowledgebase-commons/src/main/resources/hbase.yaml", "hbase");
        System.out.println(ConstContext.get("hbase.user.name", String.class));
        System.out.println(ConstContext.get("hbase.multiget.partition.size", Integer.class));
//        int size = 0;
//        if(ConstContext.containsKey("rest.server.thread.pool.size")) size = ConstContext.get("rest.server.thread.pool.size", Integer.class);
    }

    public static Map getYamlConfig(String yamlFileName, Map map) {
        if(map.size()==0){
            synchronized (map) {
                try {
                    LOG.debug("YamlUtils.getYamlConfig(),ts="+System.currentTimeMillis());
                    map = (Map) yaml.load(new FileInputStream(new File(yamlFileName)));
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
        return map;
    }



    /**
     * 初始化更新
     * @param yaml_file 要读取的yaml文件名称全路径
     * @param yaml_title_name 读取yaml文件的title名称，如"rules"
     * @param list_key_name 读取yaml文件的list key名称，如"rules.name"
     * @throws FileNotFoundException
     */
    public static void getYamlConfig(String yaml_file, String yaml_title_name, String list_key_name) throws FileNotFoundException {
        if(config.size()==0){
            updateYamlConfig(yaml_file, yaml_title_name, list_key_name);
        }
    }

    /**
     * 更新配置
     * @param yaml_file
     * @param yaml_title_name
     * @param list_key_name
     * @throws FileNotFoundException
     */
    public static Map<String, Context> updateYamlConfig(String yaml_file, String yaml_title_name, String list_key_name) throws FileNotFoundException {
        synchronized (config) {
            LOG.debug("YamlUtils.getYamlConfig(), yaml_file="+yaml_file+", ts="+System.currentTimeMillis());

            // 1.读yaml文件内容，存到一个Map里
            config = (Map) yaml.load(new FileInputStream(yaml_file));

            // 2.读yaml中的key name规则名称列表，存到一个HashSet
            Context context = new Context((Map)config.get(yaml_title_name));
            List<String> keyNameList = context.get(list_key_name, new ArrayList<String>().getClass(), null);
            Set<String> YAML_KEY_NAME_SET = new HashSet<>();  // yaml key name的set集合
            if(keyNameList.size() != 0){
                for(String keyName : keyNameList){
                    YAML_KEY_NAME_SET.add(keyName);
                }
            }

            // 3.根据log type名称列表，遍历读取详细规则，存到一个name为key，详细context为value的map
            Map<String, Context> YAML_KEY_DETAILS_MAP = new HashMap<>();    // yaml key name作为key的map，value为context
            for(String keyName : YAML_KEY_NAME_SET){
                Map map = context.get(keyName, Map.class, null);
                Context details = new Context(map);
                YAML_KEY_DETAILS_MAP.put(keyName, details);
            }

            return YAML_KEY_DETAILS_MAP;
        }
    }
}
