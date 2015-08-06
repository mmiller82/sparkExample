/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mmiller.spark;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


/**
 *
 * @author Maury Miller
 */
public class ParallelizedCollection {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Spark Application").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        List<Integer> data = Arrays.asList(31, 1, 3, 4, 5, 6, 7, 8, 9, 45, 80, 1);
        JavaRDD<Integer> distData = sc.parallelize(data);

        long numAs = distData.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer s) {
                return s.equals(1);
            }
        }).count();

        long numBs = distData.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer s) {
                return s.equals(5);
            }
        }).count();

        System.out.println("1: " + numAs + ", 5: " + numBs);
    }
}
