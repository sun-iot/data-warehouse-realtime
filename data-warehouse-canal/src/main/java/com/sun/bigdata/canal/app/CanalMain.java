package com.sun.bigdata.canal.app;

public class CanalMain {

    public static void main(String[] args) {
        CanalClient.watch("192.168.1.104", 11111, "example", "gmall_realtime.order_info");
    }
}
