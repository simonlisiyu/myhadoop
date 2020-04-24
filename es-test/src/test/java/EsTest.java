//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//import com.lsy.myhadoop.es.commons.ElasticsearchHttpConfig;
//import com.lsy.myhadoop.es.utils.HttpUtils;
//import org.springframework.http.HttpHeaders;
//
//import static com.lsy.myhadoop.es.commons.ElasticsearchHttpConst.ES_SEARCH;
//
///**
// * Created by lisiyu on 2020/4/7.
// */
//public class EsTest {
//    public static String DEPT_DATA = "5dec0ab13c,松下秘书科,平原市公安局交通警察支队松下区大队秘书科,翠缕,85082518,85082518,中环路568号\n" +
//            "a1ecee6272,玉琼中队,平原市公安局交通警察支队市南区大队玉琼路中队,翠墨,85082520,85082520,中环路568号\n" +
//            "0c0080154e,事故处,平原市公安局交通警察支队交通肇事处理处,翠云,85082282,88551194,二环东路6897号\n" +
//            "0e91f0b535,原办事处执法站,平原市公安局交通警察支队松下区大队情侣路中队原办事处执法站,大姐,85103161,85103161,中环路568号\n" +
//            "a5b4f4a0bd,塘子巷检查站,塘子巷检查站,大了,85088269,85088269,中环路568号\n" +
//            "91f7c0719f,二大队棋盘山中队,平原市公安局交通警察支队芜湖区大队棋盘山中队,戴良,85082200,85082300,中环路568号\n" +
//            "c4314d96a4,市南大队三中队,平原市公安局交通警察支队市南区大队三中队,戴权,85082520,85082520,中环路568号\n" +
//            "05886f32af,天山大队四中队,平原市公安局交通警察支队天山区大队四中队,靛儿,85082540,85082540,中环路568号\n" +
//            "8bc650267f,天山大队一中队,平原市公安局交通警察支队天山区大队一中队,定儿,85702540,85702540,中环路568号\n" +
//            "bc54727428,聚源五峰中队,平原市公安局交通警察支队聚源区大队五峰中队,豆官,87211901,87211901,中环路568号";
//
//    public static void main(String[] args) {
//        ElasticsearchHttpConfig config = new ElasticsearchHttpConfig();
//        config.setIp("10.179.117.160");
//        config.setPort("9200");
//
//        String index = "testkg";
//
//        HttpHeaders headers = new HttpHeaders();
//        headers.set("Content-Type", "application/json");
//
//        String ts = System.currentTimeMillis()+"";
//        String[] lineSplit = DEPT_DATA.split("\n");
//        for(String line : lineSplit){
//            String[] splits = line.split(",");
//            JSONObject dataJson = new JSONObject();
//            dataJson.put("obj_kg_label", "pol_dept");
//            dataJson.put("obj_id", splits[0]);
//            dataJson.put("obj_name", splits[1]);
//            dataJson.put("obj_create_at", ts);
//            dataJson.put("obj_create_by", "admin");
//            dataJson.put("obj_update_at", ts);
//            dataJson.put("obj_update_by", "admin");
//            dataJson.put("obj_is_deleted", false);
//            dataJson.put("dept_id", splits[0]);
//            dataJson.put("dept_name", splits[1]);
//            dataJson.put("dept_fullname", splits[2]);
//            dataJson.put("dept_contact", splits[3]);
//            dataJson.put("work_phone", splits[4]);
//            dataJson.put("work_fax", splits[5]);
//            dataJson.put("work_address", splits[6]);
//        }
//
//
//        String url = "http://"+config.getIp()+":"+config.getPort()+"/"+index+"/"+ES_SEARCH;
//        String str = HttpUtils.doPost(url, query, headers);
//
////        String url = "http://"+config.getIp()+":"+config.getPort()+"/_nodes";
////        String str = HttpUtils.doGet(url);
//
//        JSONObject jsonObject = JSONObject.parseObject(str, JSONObject.class);
//
//        System.out.println(jsonObject);
//    }
//}
