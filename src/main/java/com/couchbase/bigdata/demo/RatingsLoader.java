package com.couchbase.bigdata.demo;


import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.spark.StoreMode;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import static com.couchbase.bigdata.demo.Utils.COMMA_DELIMITER;
import static com.couchbase.bigdata.demo.Utils.faker;
import static com.couchbase.spark.japi.CouchbaseDocumentRDD.couchbaseDocumentRDD;
import static java.lang.String.format;

public class RatingsLoader {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger logger = Logger.getLogger(RatingsLoader.class);

        // Create a Java Spark Context.
        SparkSession spark = SparkSession
                .builder()
                .appName("ratingsLoader")
                .master("local[2]") // use the JVM as the master, great for testing
                .config("spark.couchbase.nodes", "localhost")
                .config("spark.couchbase.bucket.movies", "") // open the movies bucket with empty password (yes it is this way!)
                .config("com.couchbase.username", "Administrator")
                .config("com.couchbase.password", "password")
                // 5s default timemout
                // increase for small clusters
                .config("com.couchbase.kvTimeout", "5000")
                .config("com.couchbase.connectTimeout", "5000")
                .config("com.couchbase.socketConnect", "5000")
                .config("com.couchbase.maxRetryDelay", "5000")
                .config("com.couchbase.minRetryDelay", "5000")
                .getOrCreate();


        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // config local movies directory
        JavaRDD<String> lines = sc.textFile(System.getProperty("user.home") + "/Movies/ratings.csv").filter(line -> !line.contains("userId")); // skip first row

        JavaRDD<JsonDocument> jsonDocumentJavaRDD = lines.map(line -> {

            String[] splits = line.split(COMMA_DELIMITER);

            JsonObject rating = JsonObject.empty();

            rating.put("rating", Float.valueOf(splits[2]));

            // enrichment
            rating.put("price", faker.number().randomDouble(2, 5, 10));
            rating.put("latitude", faker.number().randomDouble(7, 24, 49));
            rating.put("longitude", faker.number().randomDouble(7, -124, -66));
            rating.put("type", "rating");

            // transformation
            rating.put("movieId", "movie_" + splits[1]);
            rating.put("userId", "user_" + splits[0]);
            rating.put("timestamp", Long.parseLong(splits[3]) * 1000); // to milliseconds


            // key ratings_movieId_userId
            return JsonDocument.create(format("rating::%s::%s", splits[1], splits[0]), rating);
        });

        logger.info(format("***** Total ratings %d", jsonDocumentJavaRDD.count()));
        // sample of documents to be persisted
        logger.info(format("***** Sample %s", jsonDocumentJavaRDD.take(1)));

        couchbaseDocumentRDD(
                jsonDocumentJavaRDD
        ).saveToCouchbase(StoreMode.UPSERT,"movies",5000,5000);


    }
}
