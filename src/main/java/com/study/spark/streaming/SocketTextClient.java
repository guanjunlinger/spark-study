package com.study.spark.streaming;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class SocketTextClient {
    public static void main(String[] args) {
        boolean start = true;
        try {
            ServerSocket serverSocket = new ServerSocket(8800);
            while (true) {
                Socket socket = serverSocket.accept();
                PrintWriter pw = new PrintWriter(socket.getOutputStream());
                Scanner scanner = new Scanner(System.in);

                while (start) {
                    String line = scanner.nextLine();
                    pw.write(line);
                }
                pw.close();
                socket.close();
            }
        } catch(IOException e){
            e.printStackTrace();
        }
    }

}
