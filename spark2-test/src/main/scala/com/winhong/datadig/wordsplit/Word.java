package com.winhong.datadig.wordsplit;/*
package com.winhong.datadig.wordsplit;


import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.winhong.gzszyy.commons.mysql.dao.ArticleDAO;
import com.winhong.gzszyy.commons.mysql.dao.impl.ArticleDAOImpl;
import com.winhong.gzszyy.commons.mysql.entity.Article;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

*/
/**
 * Created by lisiyu on 11/26/15.
 *//*

public class Word {
    private static final Logger LOGGER = LoggerFactory.getLogger(Word.class);
    private static String serverUri = Config.getWordServeUrl();
    static WebTarget target = ClientBuilder.newClient().target(serverUri);

    @SuppressWarnings("unchecked")
    public static List<String> segWithStopWords(String input) {
        List<String> ret = new ArrayList<>();
        try {
            Response response = target.request().buildPost(Entity.entity(input, MediaType.APPLICATION_FORM_URLENCODED)).invoke();
            String value = response.readEntity(String.class);
            //JSONObject value = response.readEntity(JSONObject.class);
            LOGGER.debug("wordResponse:"+value);
            //ret = (List<String>) JSONArray.toCollection(value.getJSONArray("ret"));
            ret = (List<String>) JSONArray.toCollection(JSONObject.fromObject(value).getJSONArray("ret"));
        } catch (Exception e) {
            LOGGER.error("input:" + input + "." + e.getMessage(), e);
        }
        LOGGER.debug("wordRet:"+ ret);
        return ret;
    }

    public static void main(String[] args) {

        ArticleDAO articlDao = new ArticleDAOImpl();
        List<Article> contentsList = articlDao.getAllContents();
        int a = 0;
        for (Article article : contentsList){
            String contentSplit = segWithStopWords(article.getCONTENT()).toString();
            article.setCONTENT_SPLIT(contentSplit.substring(1,contentSplit.length()-1));
            try {
                a += articlDao.updateContentSplitByNo(article);
                System.out.println(a);
            }catch (Exception ex){
                System.out.println(ex);
            }
        }



    }
}
*/
