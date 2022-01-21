package cn.doitedu.sparkrun;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Executor {

    public static void main(String[] args) throws IOException {

        ServerSocket serverSocket = new ServerSocket(19098);
        System.out.println("executor进程启动，绑定了端口：19098，准备开始接收task来运行.......");
        while(true){
            Socket socket = serverSocket.accept();
            new Thread(new ExecutorRunnable(socket)).start();
        }
    }
}
