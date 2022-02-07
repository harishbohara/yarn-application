package com.harish.yarn.application;

public class Application {
    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i < 100000; i++) {
            System.out.println("I=" + i);
            Thread.sleep(1000);
        }
    }
}
