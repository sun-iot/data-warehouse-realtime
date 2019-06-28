package com.sun.bigdata.canal.app;

public class CanalMain {

    public static void main(String[] args) {
        CanalClient.watch("hadoop1",11111,"example1","gmall0105.order_info");
    }
}
