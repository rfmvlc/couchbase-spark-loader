package com.couchbase.bigdata.demo;


import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.spark.StoreMode;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.couchbase.spark.japi.CouchbaseDocumentRDD.couchbaseDocumentRDD;
import static java.lang.String.format;

public class MoviesLoader {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger logger = Logger.getLogger(MoviesLoader.class);

        // Create a Java Spark Context.
        SparkSession spark = SparkSession
                .builder()
                .appName("moviesLoader")
                .master("local[2]") // use the JVM as the master, great for testing
                .config("spark.couchbase.nodes", "localhost")
                .config("spark.couchbase.bucket.movies", "") // open the movies bucket with empty password (yes it is this way!)
                .config("com.couchbase.username", "Administrator")
                .config("com.couchbase.password", "password")
                // 1s default timemout
                // increase for small clusters
                .config("com.couchbase.kvTimeout", "1000")
                .config("com.couchbase.connectTimeout", "1000")
                .config("com.couchbase.socketConnect", "1000")
                .config("com.couchbase.maxRetryDelay", "1000")
                .config("com.couchbase.minRetryDelay", "1000")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // config local movies directory
        JavaRDD<String> lines = sc.textFile(System.getProperty("user.home") + "/Movies/movies.csv").filter(line -> !line.contains("movieId")); // skip first row


        // crate rdd with json jsonDocumentJavaRDD
        JavaRDD<JsonDocument> jsonDocumentJavaRDD = lines.map(line -> {

            JsonObject movie = JsonObject.empty();
            String[] splits = line.split(Utils.COMMA_DELIMITER);

            // enrichment
            movie.put("type", "movie");

            // transformation

            String originalTitle = splits[1].replace("\"", "");

            // if title contains year
            Matcher m = Pattern.compile("\\(([1-2][0-9][0-9][0-9])\\)").matcher(splits[1]);
            if (m.find()) {
                movie.put("year", Integer.parseInt(m.group(1)));
                originalTitle = originalTitle.replace(m.group(0), "").trim();

            }

            movie.put("title", originalTitle);

            // genres to array
            movie.put("genres", Arrays.asList(splits[2].split("\\|")));

            // key movie_movieId
            return JsonDocument.create(format("movie::%s", splits[0]), movie);
        });

        logger.info(format("***** Total movies %d", jsonDocumentJavaRDD.count()));
        // sample of documents to be persisted
        logger.info(format("***** Sample %s", jsonDocumentJavaRDD.take(1)));

        couchbaseDocumentRDD(
                jsonDocumentJavaRDD
        ).saveToCouchbase(StoreMode.UPSERT,"movies",1000,1000);

    }

}
