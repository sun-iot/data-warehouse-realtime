package com.sun.bigdata.canal.app;

public class CanalMain2 {

    public static void main(String[] args) {
        CanalClient.watch("hadoop104", 11111, "example2", "gmall0105.user_info");
    }
}
