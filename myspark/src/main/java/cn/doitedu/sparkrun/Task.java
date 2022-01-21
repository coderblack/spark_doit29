package cn.doitedu.sparkrun;

import java.io.Serializable;

public class Task implements Serializable {
    private String rdd;
    private int partitionId;

    public Task() {
    }

    public Task(String rdd, int partitionId) {
        this.rdd = rdd;
        this.partitionId = partitionId;
    }

    public void runTask() throws InterruptedException {
        for(int i=0;i<1000;i++){
            Thread.sleep(1000);
            System.out.println("我是一个task，我运行着，我负责的rdd是 : " + this.rdd +",我负责的分区是：" + this.partitionId);
        }
    }

}
