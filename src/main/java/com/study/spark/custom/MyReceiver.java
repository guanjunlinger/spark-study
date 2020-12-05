package com.study.spark.custom;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class MyReceiver extends Receiver<String> {

    String host;
    int port;

    public MyReceiver(String host_, int port_) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        host = host_;
        port = port_;
    }

    @Override
    public void onStart() {
        new Thread(this::receive).start();
    }

    @Override
    public void onStop() {
    }

    private void receive() {
        Socket socket;
        String userInput;
        try {
            socket = new Socket(host, port);
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            while (!isStopped() && (userInput = reader.readLine()) != null) {
                System.out.println("Received data:" + userInput );
                store(userInput);
            }
            reader.close();
            socket.close();
            restart("Trying to connect again");
        } catch (ConnectException ce) {
            restart("Could not connect", ce);
        } catch (Throwable t) {
            restart("Error receiving data", t);
        }
    }
}
