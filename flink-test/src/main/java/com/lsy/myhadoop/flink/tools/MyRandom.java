package com.lsy.myhadoop.flink.tools;

import java.util.Random;

public class MyRandom extends Random {
    public Integer GetGs(Integer x1, Integer x2){
        double q = nextGaussian();
        double result = (q+(x2+x1)/2)*(2*x2)/(x2+x1)/2 ;
        long round = Math.round(result);
        return  (int) round;
    }
}
