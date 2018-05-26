package com.couchbase.bigdata.demo;


import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.google.common.collect.Lists;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.couchbase.spark.japi.CouchbaseDocumentRDD.couchbaseDocumentRDD;
import static java.lang.String.format;
import static org.apache.commons.lang.math.RandomUtils.nextFloat;

public class RatingsLoader {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger logger = Logger.getLogger(RatingsLoader.class);

        // Create a Java Spark Context.
        SparkSession spark = SparkSession
                .builder()
                .appName("ratingsLoader")
                .master("local[*]") // use the JVM as the master, great for testing
                .config("spark.couchbase.nodes", "localhost")
                .config("spark.couchbase.bucket.movies", "") // open the movies bucket with empty password (yes it is this way!)
                .config("com.couchbase.username", "Administrator")
                .config("com.couchbase.password", "password")
                // 60s to avoid timeouts on small clusters
                .config("com.couchbase.kvTimeout", "60000")
                .config("com.couchbase.connectTimeout", "60000")
                .config("com.couchbase.socketConnect", "60000")
                .config("com.couchbase.maxRetryDelay", "60000")
                .config("com.couchbase.minRetryDelay", "60000")
                .getOrCreate();


        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // small data set for small clusters
        JavaRDD<String> lines = sc.textFile("in/ratings.csv").filter(line -> !line.contains("userId"));
        // uncomment for full data set
        // JavaRDD<String> lines = sc.textFile("in/ml-latest/movies.csv").filter(line -> !line.contains("movieId"));

        JavaRDD<JsonDocument> jsonDocumentJavaRDD = lines.map(line -> {

            String[] splits = line.split(",");
            JsonObject rating = JsonObject.empty();
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(Long.parseLong(splits[3]) * 1000);


            rating.put("rating", Float.valueOf(splits[2]));

            // enrichment
            rating.put("price", getRandomPrice());
            rating.put("country", getRandomCountry());
            rating.put("type", "rating");


            // transformation
            rating.put("movieId", "movie_" + splits[1]);
            rating.put("userId", "user_" + splits[0]);
            rating.put("year", calendar.get(Calendar.YEAR));


            // key ratings_movieId_userId
            return JsonDocument.create(format("rating_%s_%s", splits[1], splits[0]), rating);
        });

        logger.info(format("***** Total ratings %d", jsonDocumentJavaRDD.count()));
        // sample of documents to be persisted
        logger.info(format("***** Sample %s", jsonDocumentJavaRDD.take(1)));

        couchbaseDocumentRDD(
                jsonDocumentJavaRDD
        ).saveToCouchbase();


    }

    private static double getRandomPrice() {

        double randomPrice = 8.99 +
                RandomUtils.nextFloat() * (19.99 - 8.99);
        int intPrice = (int) (randomPrice * 100);
        return intPrice / 100d;

    }


    public static String getRandomCountry() {
        int length = Locale.getISOCountries().length;
        return Locale.getISOCountries()[RandomUtils.nextInt(length - 1)];
    }
}
