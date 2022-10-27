package com.iamxgw.mydb.backend.utils;

public class Panic {
    public static void panic(Exception e) {
        e.printStackTrace();
        System.exit(1);
    }
}
