package com.gy.hbase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * created by yangyu on 2019-11-08
 */
@SpringBootApplication
public class Application {


    public static void main(String...args){
        SpringApplication application = new SpringApplication(Application.class);
        application.run(args);
    }
}
