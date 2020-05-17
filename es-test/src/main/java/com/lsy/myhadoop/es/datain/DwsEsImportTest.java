package com.lsy.myhadoop.es.datain;

import com.alibaba.fastjson.JSONObject;
import com.lsy.myhadoop.es.client.ElasticSearchTransportClient;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import redis.clients.jedis.Jedis;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 知识图谱，测试数据，导入ES
 * Created by lisiyu on 2017/1/22.
 */
public class DwsEsImportTest {
    final static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

    public static String DEPT_DATA = "5dec0ab13c,松下秘书科,平原市公安局交通警察支队松下区大队秘书科,翠缕,85082518,85082518,中环路568号\n" +
            "a1ecee6272,玉琼中队,平原市公安局交通警察支队市南区大队玉琼路中队,翠墨,85082520,85082520,中环路568号\n" +
            "0c0080154e,事故处,平原市公安局交通警察支队交通肇事处理处,翠云,85082282,88551194,二环东路6897号\n" +
            "0e91f0b535,原办事处执法站,平原市公安局交通警察支队松下区大队情侣路中队原办事处执法站,大姐,85103161,85103161,中环路568号\n" +
            "a5b4f4a0bd,塘子巷检查站,塘子巷检查站,大了,85088269,85088269,中环路568号\n" +
            "91f7c0719f,二大队棋盘山中队,平原市公安局交通警察支队芜湖区大队棋盘山中队,戴良,85082200,85082300,中环路568号\n" +
            "c4314d96a4,市南大队三中队,平原市公安局交通警察支队市南区大队三中队,戴权,85082520,85082520,中环路568号\n" +
            "05886f32af,天山大队四中队,平原市公安局交通警察支队天山区大队四中队,靛儿,85082540,85082540,中环路568号\n" +
            "8bc650267f,天山大队一中队,平原市公安局交通警察支队天山区大队一中队,定儿,85702540,85702540,中环路568号\n" +
            "bc54727428,聚源五峰中队,平原市公安局交通警察支队聚源区大队五峰中队,豆官,87211901,87211901,中环路568号";
    public static String POL_DATA = "37010219750129****,58025,交警,艾官,男,1975-01-29,中共党员,13361086197,0933-91752854,5dec0ab13c\n" +
            "37010219690603****,33992,交警,板儿,男,1969-06-03,群众,15508669965,0933-23068499,a1ecee6272\n" +
            "37011119700619****,22745,交警,伴鹤,男,1970-06-19,中共党员,13361086915,0933-74204092,0c0080154e\n" +
            "37010519621108****,26528,交警,包勇,男,1962-11-08,中共党员,15605419857,0933-83838613,0e91f0b535\n" +
            "37018119721011****,80831,交警,宝钗,男,1972-10-11,中共党员,15605419877,0933-59336002,a5b4f4a0bd\n" +
            "37132619811010****,20309,交警,宝蟾,女,1981-10-10,中共党员,13361086992,0933-86750235,91f7c0719f\n" +
            "37091919810926****,50398,交警,宝官,男,1981-09-26,中共党员,19863486906,0933-45077258,c4314d96a4\n" +
            "37012319760112****,14912,交警,宝玉,男,1976-01-12,中共党员,13105416606,0933-93126587,05886f32af\n" +
            "37010319780806****,22365,交警,宝珠,男,1978-08-06,中共党员,13361088267,0933-27544456,8bc650267f\n" +
            "37080219690421****,40098,交警,抱琴,女,1969-04-21,中共党员,13361087897,0933-66949419,bc54727428";
    public static String DRI_DATA = "37010519861226****,身份证,方椿,女,1996-07-28,平原市天山区黄岗东路*号*号楼***号,83312707,15053161762,fangchun@xx.com\n" +
            "37010519880418****,身份证,方杏,女,1993-10-18,远东省平原市市南区****,83312707,13864146297,fangxing@xx.com\n" +
            "37120219800210****,身份证,芳官,男,1965-01-29,远东省平原市芜湖区凤城街道办事处****,1,13021227627,fangguan@xx.com\n" +
            "37010519980420****,身份证,翡翠,女,1957-02-17,平原市天山区板桥庄***,0,13054805215,feicui@xx.com\n" +
            "37290119881123****,身份证,丰儿,男,1952-08-10,平原市松下区舜华路***号,1,13054805216,fenger@xx.com\n" +
            "37010419951103****,身份证,封氏,男,1964-04-20,平原市槐荫区美里湖办事处新沙王庄村***号,0,13054805213,fengshi@xx.com\n" +
            "37018119970604****,身份证,封肃,男,1963-01-08,远东省章丘市绣惠镇回东村棋盘街***号,87928850,13806405525,fengsu@xx.com\n" +
            "37292619890120****,身份证,冯仆,男,1990-06-04,平原市历城区兴港路***号*号楼*单元****号,1,15689731714,fengpu@xx.com\n" +
            "43048119791007****,身份证,冯唐,男,1986-12-14,平原市南部山区西营镇***,6441792,18363451919,fengtang@xx.com";
    public static String CAR_DATA = "升仕牌,轻型普通货车,白灰,货车,AHX9**,ZT300-X1,2019-09-30,货运,37010019512346\n" +
            "别克牌,小客车,白,小型汽车,A5N0**,SGM7153DAAA,2015-12-10,客运,37060116139867\n" +
            "宝骏牌,小客车,白,小型汽车,SBF6**,LZW6420CJY,2018-11-22,客运,37120019004611\n" +
            "荣威牌,小客车,白,小型汽车,A9A1**,CSA6452NEAN,2019-08-12,客运,37010019518945\n" +
            "丰田牌,小客车,白,小型汽车,A0BG**,TV7184GL-iHEV,2018-06-15,客运,37010018495213\n" +
            "大众汽车牌,小客车,白,小型汽车,AT28**,SVW6451BGD,2018-01-11,客运,37010018103759\n" +
            "海马牌,小客车,白,小型汽车,AJ2V**,HMC7168D5S0,2018-06-15,客运,37010018903918\n" +
            "北京现代牌,小客车,白,小型汽车,A9BD**,BH7166TAS,2019-03-21,客运,37010019515032\n" +
            "雪佛兰牌,小客车,白,小型汽车,A8B6**,SGM7154EAA1,2018-10-16,客运,37010018855770";
    public static String DRI_CAR_REL = "37010519861226****,37010019512346,元,100000,2019-09-30\n" +
            "37010519880418****,37060116139867,元,100000,2019-09-30\n" +
            "37120219800210****,37120019004611,元,100000,2019-09-30\n" +
            "37010519980420****,37010019518945,元,100000,2019-09-30\n" +
            "37290119881123****,37010018495213,元,100000,2019-09-30\n" +
            "37010419951103****,37010018103759,元,100000,2019-09-30\n" +
            "37018119970604****,37010018903918,元,100000,2019-09-30\n" +
            "37292619890120****,37010019515032,元,100000,2019-09-30\n" +
            "43048119791007****,37010018855770,元,100000,2019-09-30\n";
    public static String POL_DEPT_REL = "58025,5dec0ab13c,1995-07-16,城区管理执勤\n" +
            "33992,a1ecee6272,1989-07-16,事故处理\n" +
            "22745,0c0080154e,1989-07-16,事故处理\n" +
            "26528,0e91f0b535,1989-07-16,事故处理\n" +
            "80831,a5b4f4a0bd,1989-07-16,事故处理\n" +
            "20309,91f7c0719f,1989-07-16,事故处理\n" +
            "50398,c4314d96a4,1995-07-16,管理岗\n" +
            "14912,05886f32af,1996-07-16,文职\n" +
            "22365,8bc650267f,2008-07-16,事故处理\n" +
            "40098,bc54727428,1999-07-16,事故处理";



    public static void main(String[] args) throws UnknownHostException, ParseException {
        String indexAgg = "dws_kg_vertex_obj_agg";
        String type = "f";
        String ip = "10.179.117.160";

        Client client = ElasticSearchTransportClient.getInstance(ip);
        Jedis jedis = new Jedis("10.179.18.10");

        //开启批量插入
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        long ts = System.currentTimeMillis();

        String indexDept = "dws_kg_vertex_dept";
        String indexPolice = "dws_kg_vertex_police";
        String indexDriver = "dws_kg_vertex_driver";
        String indexVehicle = "dws_kg_vertex_vehicle";
        String indexDriVehRel = "dws_kg_edge_dri_veh_rel";
        String indexPolDeptRel = "dws_kg_edge_pol_dept_rel";

        int inTag = 2; // 1=index, 2=agg,3=index+agg

        putDeptData(DEPT_DATA, bulkRequest, jedis, indexAgg, indexDept,type,ts, inTag);
        putPoliceData(POL_DATA, bulkRequest, jedis, indexAgg, indexPolice,type,ts, inTag);
        putDriverData(DRI_DATA, bulkRequest, jedis, indexAgg, indexDriver,type,ts, inTag);
        putVehicleData(CAR_DATA, bulkRequest, jedis, indexAgg, indexVehicle,type,ts, inTag);
//        putPoliceDeptRelData(POL_DEPT_REL, bulkRequest, indexAgg, indexPolDeptRel,type,ts);
//        putDriverVehicleRelData(DRI_CAR_REL, bulkRequest, indexAgg, indexDriVehRel,type,ts);

        bulkRequest.execute().actionGet();

        System.out.println("import finished.");
        jedis.close();
        client.close();

    }


    public static void putDeptData(String data, BulkRequestBuilder bulkRequest, Jedis jedis,
                                   String indexAgg, String index, String type,
                                   long ts, int inTag) {

        String[] lineSplit = data.split("\n");
        for (String line : lineSplit) {
            String[] splits = line.split(",");
            JSONObject aggJson = new JSONObject();
            JSONObject dataJson = new JSONObject();
            JSONObject objJson = new JSONObject();

            dataJson.put("obj_create_at", ts);
            dataJson.put("obj_create_by", "admin");
            dataJson.put("obj_update_at", ts);
            dataJson.put("obj_update_by", "admin");
            dataJson.put("obj_is_deleted", false);
            /**
             * 部门
             */
            dataJson.put("obj_kg_class", "pol");
            dataJson.put("obj_kg_label", "pol_dept");
            dataJson.put("obj_id", splits[0]);
            dataJson.put("obj_name", splits[1]);
            objJson.put("dept_id", splits[0]);
            objJson.put("dept_name", splits[1]);
            objJson.put("dept_fullname", splits[2]);
            objJson.put("dept_contact", splits[3]);
            objJson.put("work_phone", splits[4]);
            objJson.put("work_fax", splits[5]);
            objJson.put("work_address", splits[6]);

            aggJson.putAll(dataJson);
            aggJson.put("key1", objJson.getString("dept_id"));
            aggJson.put("key2", objJson.getString("dept_name"));
            aggJson.put("key3", "");
            aggJson.put("key4", "");
            aggJson.put("key5", "");
            aggJson.put("kv_json", objJson.toString());
            dataJson.putAll(objJson);

            String id = dataJson.getString("obj_id");
            System.out.println(id);
            System.out.println(dataJson);

            switch (inTag){
                case 1:
                    bulkRequest.add(new UpdateRequest(index, type, id).doc(dataJson).docAsUpsert(true));
                    break;
                case 2:
                    bulkRequest.add(new UpdateRequest(indexAgg, type, id).doc(aggJson).docAsUpsert(true));
                    break;
                case 3:
                    bulkRequest.add(new UpdateRequest(index, type, id).doc(dataJson).docAsUpsert(true));
                    bulkRequest.add(new UpdateRequest(indexAgg, type, id).doc(aggJson).docAsUpsert(true));
                    break;
                default:
                    bulkRequest.add(new UpdateRequest(index, type, id).doc(dataJson).docAsUpsert(true));
                    bulkRequest.add(new UpdateRequest(indexAgg, type, id).doc(aggJson).docAsUpsert(true));
                    break;
            }

            putAggJsonKeysToCache(jedis, aggJson);
        }
    }

    public static void putPoliceData(String data, BulkRequestBuilder bulkRequest, Jedis jedis,
                                     String indexAgg, String index, String type,
                                     long ts, int inTag) throws ParseException {

        String[] lineSplit = data.split("\n");
        for (String line : lineSplit) {
            String[] splits = line.split(",");
            JSONObject aggJson = new JSONObject();
            JSONObject dataJson = new JSONObject();
            JSONObject objJson = new JSONObject();

            dataJson.put("obj_create_at", ts);
            dataJson.put("obj_create_by", "admin");
            dataJson.put("obj_update_at", ts);
            dataJson.put("obj_update_by", "admin");
            dataJson.put("obj_is_deleted", false);
            /**
             * 警员
             */
            dataJson.put("obj_kg_class", "pol");
            dataJson.put("obj_kg_label", "pol_police");
            dataJson.put("obj_id", splits[1]);
            dataJson.put("obj_name", splits[3]);
            objJson.put("hum_id_num", splits[0]);
            objJson.put("pol_id", splits[1]);
            objJson.put("pol_type", splits[2]);
            objJson.put("hum_name", splits[3]);
            objJson.put("hum_sex", splits[4]);
            objJson.put("hum_birthday", df.parse(splits[5]).getTime());
            objJson.put("political_status", splits[6]);
            objJson.put("hum_mobile", splits[7]);
            objJson.put("work_phone", splits[8]);
            objJson.put("dept_id", splits[9]);

            aggJson.putAll(dataJson);
            aggJson.put("key1", objJson.getString("hum_id_num"));
            aggJson.put("key2", objJson.getString("pol_id"));
            aggJson.put("key3", objJson.getString("hum_name"));
            aggJson.put("key4", objJson.getString("hum_mobile"));
            aggJson.put("key5", objJson.getString("dept_id"));
            aggJson.put("kv_json", objJson.toString());
            dataJson.putAll(objJson);

            String id = dataJson.getString("obj_id");
            System.out.println(id);
            System.out.println(dataJson);
            switch (inTag){
                case 1:
                    bulkRequest.add(new UpdateRequest(index, type, id).doc(dataJson).docAsUpsert(true));
                    break;
                case 2:
                    bulkRequest.add(new UpdateRequest(indexAgg, type, id).doc(aggJson).docAsUpsert(true));
                    break;
                case 3:
                    bulkRequest.add(new UpdateRequest(index, type, id).doc(dataJson).docAsUpsert(true));
                    bulkRequest.add(new UpdateRequest(indexAgg, type, id).doc(aggJson).docAsUpsert(true));
                    break;
                default:
                    bulkRequest.add(new UpdateRequest(index, type, id).doc(dataJson).docAsUpsert(true));
                    bulkRequest.add(new UpdateRequest(indexAgg, type, id).doc(aggJson).docAsUpsert(true));
                    break;
            }
            putAggJsonKeysToCache(jedis, aggJson);
        }
    }

    public static void putDriverData(String data, BulkRequestBuilder bulkRequest, Jedis jedis,
                                     String indexAgg, String index, String type,
                                     long ts, int inTag) throws ParseException {

        String[] lineSplit = data.split("\n");
        for (String line : lineSplit) {
            String[] splits = line.split(",");
            JSONObject aggJson = new JSONObject();
            JSONObject dataJson = new JSONObject();
            JSONObject objJson = new JSONObject();

            dataJson.put("obj_create_at", ts);
            dataJson.put("obj_create_by", "admin");
            dataJson.put("obj_update_at", ts);
            dataJson.put("obj_update_by", "admin");
            dataJson.put("obj_is_deleted", false);
            /**
             * 驾驶员
             */dataJson.put("obj_kg_class", "hum");
            dataJson.put("obj_kg_label", "hum_driver");
            dataJson.put("obj_id", splits[0]);
            dataJson.put("obj_name", splits[2]);
            objJson.put("hum_id_num", splits[0]);
            objJson.put("hum_id_type", splits[1]);
            objJson.put("hum_name", splits[2]);
            objJson.put("hum_sex", splits[3]);
            objJson.put("hum_birthday", df.parse(splits[4]).getTime());
            objJson.put("hum_address", splits[5]);
            objJson.put("hum_phone", splits[6]);
            objJson.put("hum_mobile", splits[7]);
            objJson.put("hum_email", splits[8]);

            aggJson.putAll(dataJson);
            aggJson.put("key1", objJson.getString("hum_id_num"));
            aggJson.put("key2", objJson.getString("hum_name"));
            aggJson.put("key3", objJson.getString("hum_mobile"));
            aggJson.put("key4", "");
            aggJson.put("key5", "");
            aggJson.put("kv_json", objJson.toString());
            dataJson.putAll(objJson);

            String id = dataJson.getString("obj_id");
            System.out.println(id);
            System.out.println(dataJson);
            switch (inTag){
                case 1:
                    bulkRequest.add(new UpdateRequest(index, type, id).doc(dataJson).docAsUpsert(true));
                    break;
                case 2:
                    bulkRequest.add(new UpdateRequest(indexAgg, type, id).doc(aggJson).docAsUpsert(true));
                    break;
                case 3:
                    bulkRequest.add(new UpdateRequest(index, type, id).doc(dataJson).docAsUpsert(true));
                    bulkRequest.add(new UpdateRequest(indexAgg, type, id).doc(aggJson).docAsUpsert(true));
                    break;
                default:
                    bulkRequest.add(new UpdateRequest(index, type, id).doc(dataJson).docAsUpsert(true));
                    bulkRequest.add(new UpdateRequest(indexAgg, type, id).doc(aggJson).docAsUpsert(true));
                    break;
            }
            putAggJsonKeysToCache(jedis, aggJson);
        }
    }

    public static void putVehicleData(String data, BulkRequestBuilder bulkRequest, Jedis jedis,
                                      String indexAgg, String index, String type,
                                      long ts, int inTag) throws ParseException {

        String[] lineSplit = data.split("\n");
        for (String line : lineSplit) {
            String[] splits = line.split(",");
            JSONObject aggJson = new JSONObject();
            JSONObject dataJson = new JSONObject();
            JSONObject objJson = new JSONObject();

            dataJson.put("obj_create_at", ts);
            dataJson.put("obj_create_by", "admin");
            dataJson.put("obj_update_at", ts);
            dataJson.put("obj_update_by", "admin");
            dataJson.put("obj_is_deleted", false);
            /**
             * 机动车
             */
            dataJson.put("obj_kg_class", "veh");
            dataJson.put("obj_kg_label", "veh_vehicle");
            dataJson.put("obj_id", splits[8]);
            dataJson.put("obj_name", splits[3]+splits[4]);
            objJson.put("manufacture", splits[0]);
            objJson.put("veh_type", splits[1]);
            objJson.put("veh_color", splits[2]);
            objJson.put("veh_plate_type", splits[3]);
            objJson.put("veh_plate_num", splits[4]);
            objJson.put("veh_mode", splits[5]);
            objJson.put("produce_date", df.parse(splits[6]).getTime());
            objJson.put("veh_use", splits[7]);

            aggJson.putAll(dataJson);
            aggJson.put("key1", objJson.getString("manufacture"));
            aggJson.put("key2", objJson.getString("veh_type"));
            aggJson.put("key3", objJson.getString("veh_color"));
            aggJson.put("key4", objJson.getString("veh_plate_type"));
            aggJson.put("key5", objJson.getString("veh_plate_num"));
            aggJson.put("kv_json", objJson.toString());
            dataJson.putAll(objJson);

            String id = dataJson.getString("obj_id");
            System.out.println(id);
            System.out.println(dataJson);
            switch (inTag){
                case 1:
                    bulkRequest.add(new UpdateRequest(index, type, id).doc(dataJson).docAsUpsert(true));
                    break;
                case 2:
                    bulkRequest.add(new UpdateRequest(indexAgg, type, id).doc(aggJson).docAsUpsert(true));
                    break;
                case 3:
                    bulkRequest.add(new UpdateRequest(index, type, id).doc(dataJson).docAsUpsert(true));
                    bulkRequest.add(new UpdateRequest(indexAgg, type, id).doc(aggJson).docAsUpsert(true));
                    break;
                default:
                    bulkRequest.add(new UpdateRequest(index, type, id).doc(dataJson).docAsUpsert(true));
                    bulkRequest.add(new UpdateRequest(indexAgg, type, id).doc(aggJson).docAsUpsert(true));
                    break;
            }
            putAggJsonKeysToCache(jedis, aggJson);
        }
    }

    public static void putPoliceDeptRelData(String data, BulkRequestBuilder bulkRequest,
                                            String indexAgg, String index, String type,
                                            long ts) throws ParseException {

        String[] lineSplit = data.split("\n");
        for (String line : lineSplit) {
            String[] splits = line.split(",");
            JSONObject dataJson = new JSONObject();
            JSONObject objJson = new JSONObject();

            dataJson.put("obj_create_at", ts);
            dataJson.put("obj_create_by", "admin");
            dataJson.put("obj_update_at", ts);
            dataJson.put("obj_update_by", "admin");
            dataJson.put("obj_is_deleted", false);
            /**
             * 警部门关系
             */
            dataJson.put("obj_kg_label", "police_dept_rel");
            dataJson.put("obj_id", splits[0]+splits[1]);
            objJson.put("obj_from_id", splits[0]);
            objJson.put("obj_to_id", splits[1]);
            objJson.put("pol_first_date", df.parse(splits[2]).getTime());
            objJson.put("pol_position_type", splits[3]);
            dataJson.putAll(objJson);

            String id = dataJson.getString("obj_id");
            System.out.println(id);
            System.out.println(dataJson);
            bulkRequest.add(new UpdateRequest(index, type, id).doc(dataJson).docAsUpsert(true));
        }
    }

    public static void putDriverVehicleRelData(String data, BulkRequestBuilder bulkRequest,
                                               String indexAgg, String index, String type,
                                               long ts) throws ParseException {

        String[] lineSplit = data.split("\n");
        for (String line : lineSplit) {
            String[] splits = line.split(",");
            JSONObject dataJson = new JSONObject();
            JSONObject objJson = new JSONObject();

            dataJson.put("obj_create_at", ts);
            dataJson.put("obj_create_by", "admin");
            dataJson.put("obj_update_at", ts);
            dataJson.put("obj_update_by", "admin");
            dataJson.put("obj_is_deleted", false);
            /**
             * 人车关系
             */
            dataJson.put("obj_kg_label", "driver_vehicle_rel");
            dataJson.put("obj_id", splits[0]+splits[1]);
            objJson.put("obj_from_id", splits[0]);
            objJson.put("obj_to_id", splits[1]);
            objJson.put("veh_sale_company", splits[2]);
            objJson.put("veh_sale_price", splits[3]);
            objJson.put("veh_sale_date", df.parse(splits[4]).getTime());
            dataJson.putAll(objJson);

            String id = dataJson.getString("obj_id");
            System.out.println(id);
            System.out.println(dataJson);
            bulkRequest.add(new UpdateRequest(index, type, id).doc(dataJson).docAsUpsert(true));
        }
    }

    public static void putAggJsonKeysToCache(Jedis jedis, JSONObject aggJson){
        System.out.println(aggJson);
        jedis.hset("knowledgebase:keyword:"+aggJson.getString("key1"), "null", "");
        jedis.hset("knowledgebase:keyword:"+aggJson.getString("key2"), "null", "");
        jedis.hset("knowledgebase:keyword:"+aggJson.getString("key3"), "null", "");
        jedis.hset("knowledgebase:keyword:"+aggJson.getString("key4"), "null", "");
        jedis.hset("knowledgebase:keyword:"+aggJson.getString("key5"), "null", "");
    }


}
