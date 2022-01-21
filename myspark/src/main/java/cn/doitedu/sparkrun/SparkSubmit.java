package cn.doitedu.sparkrun;

import java.io.IOException;

public class SparkSubmit {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("准备提交应用.......");

        new Thread(){
            @Override
            public void run() {
                try {
                    Driver.main(args);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        System.out.println("我已经把driver线程给启动好了，我又要去干别的了......");


        Thread.sleep(Long.MAX_VALUE);
    }
}
