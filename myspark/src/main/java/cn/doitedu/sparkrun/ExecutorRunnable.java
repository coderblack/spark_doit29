package cn.doitedu.sparkrun;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.Socket;

public class ExecutorRunnable implements Runnable {

    Socket socket;

    public ExecutorRunnable(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {

        try {
            InputStream inputStream = socket.getInputStream();
            ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
            Task task = (Task) objectInputStream.readObject();
            task.runTask();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
