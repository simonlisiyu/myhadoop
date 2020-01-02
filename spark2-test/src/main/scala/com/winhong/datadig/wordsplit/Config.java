package com.winhong.datadig.wordsplit;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.ibatis.io.Resources;

/**
 * Created by root on 11/26/15.
 */
public class Config {
    private static String wordServeUrl = null;
    public static String getWordServeUrl() {
        if (wordServeUrl == null){
            Properties prop = new Properties();
            try {
                InputStream inputStream = Resources.getResourceAsStream("all.properties");
                prop.load(inputStream);
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            wordServeUrl = prop.getProperty( "wordServer" ).trim();
        }
        return wordServeUrl;
    }
    public static void main(String[] args) {
        System.out.println(getWordServeUrl());
    }
}
