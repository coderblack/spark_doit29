package cn.doitedu.sparkrun;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashSet;

public class Driver {
    public static void main(String[] args) throws IOException {

        // 持有一个executor的地址列表
        String[] executors = {"doit01:19098","doit02:19098","doit03:19098"};


        // 模拟DAGScheduler的功能： 将rdd转换逻辑切分stage，生成taskset
        // stage0 的 3个task
        Task task0_0 = new Task("rdd5", 0);
        Task task0_1 = new Task("rdd5", 1);
        Task task0_2 = new Task("rdd5", 2);
        HashSet<Task> taskSet0 = new HashSet<>();
        taskSet0.add(task0_0);
        taskSet0.add(task0_1);
        taskSet0.add(task0_2);


        // stage1 的 3个task
        Task task1_0 = new Task("rdd8", 0);
        Task task1_1 = new Task("rdd8", 1);
        HashSet<Task> taskSet1 = new HashSet<>();
        taskSet1.add(task1_0);
        taskSet1.add(task1_1);

        // 模拟taskScheduler的功能： 先提交stage0的task 到executor上去执行
        int i=0;
        for (Task task : taskSet0) {
            // doit01:19098
            String[] executorHostPort = executors[i].split(":");
            i++;

            // 创建到executor的连接
            Socket socket = new Socket(executorHostPort[0], Integer.parseInt(executorHostPort[1]));
            OutputStream out = socket.getOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(out);
            objectOutputStream.writeObject(task);
        }


    }
}
