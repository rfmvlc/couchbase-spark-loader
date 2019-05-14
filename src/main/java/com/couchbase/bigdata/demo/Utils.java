package com.couchbase.bigdata.demo;

import com.github.javafaker.Faker;


public class Utils {

    // a regular expression which matches commas but not commas within double quotations
    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
    public static Faker faker = new Faker();

}
