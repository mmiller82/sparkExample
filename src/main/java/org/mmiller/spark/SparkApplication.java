/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mmiller.spark;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;


/**
 *
 * @author Maury Miller
 */
public class SparkApplication {
	/** The logger. */
	private static final Logger LOGGER = LoggerFactory.getLogger(SparkApplication.class);
    
    public static void main(String[] args) {
        
        String logFile = "textfile"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Spark Application").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                return s.contains("a");
            }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                return s.contains("b");
            }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
        
        findErrors(sc);
        
    }
    
    public static void transform(JavaSparkContext sc) {
        JavaRDD<String> textFile = sc.textFile("textfile");
        
        JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(s.split(" "));
            }
        });
        
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<>(s, 1);
            }
        });
        
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });
        
        System.out.println("Counts:\n" + counts.toString());
        
        counts.saveAsTextFile("counts");       
    }
    
    
    public static void findErrors(JavaSparkContext sc) {
        JavaRDD<String> textFile = sc.textFile("storm.log");
        JavaRDD<String> errors = textFile.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                return s.contains("ERROR");
            }
        });
        
        // Count all the errors
        LOGGER.info("Count: " + errors.count());
        
        // Count errors mentioning PersistenceException
        long count = errors.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                return s.contains("PersistenceException");
            }
        }).count();
        
        // Fetch the PersistenceException errors as an array of strings
        errors.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                return s.contains("PersistenceException");
            }
        }).collect();

        System.out.println("");
        System.out.println(errors.toArray().toString());
        System.out.println("");
        System.out.println(errors.take((int)count));
    }
}
