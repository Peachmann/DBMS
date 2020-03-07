package com.client.db;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;

import com.client.login.LoginController;

public class Listener implements Runnable {

	private static String hostname, username;
	private static int port;
	public DBController controller;
	private Socket socket;
	private static ObjectOutputStream oos;
	private OutputStream outputStream;
	private InputStream is;
    private ObjectInputStream input;
	
    public Listener(String hostname, int port, String username, DBController controller) {
        this.hostname = hostname;
        this.port = port;
        Listener.username = username;
        this.controller = controller;
    }
	
    public void run() {
        try {
            socket = new Socket(hostname, port);
            LoginController.getInstance().showScene();
            outputStream = socket.getOutputStream();
            oos = new ObjectOutputStream(outputStream);
            is = socket.getInputStream();
            input = new ObjectInputStream(is);
        } catch (IOException e) {
            LoginController.getInstance().showErrorDialog("Could not connect to server");
        }
        
        try {
            while (socket.isConnected()) {
            	// do stuff here
            }
        }
        catch (Exception e) {
        	e.printStackTrace();
        }

    }
    
}
