package com.client.db;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;

import com.client.login.LoginController;

import message.Message;
import message.MessageType;

public class Listener implements Runnable {

	private static String hostname, username;
	private static int port;
	public DBController controller;
	private Socket socket;
	private static ObjectOutputStream oos;
	private OutputStream outputStream;
	private InputStream is;
    private ObjectInputStream input;
    private static String dbPath, indexPath;
    
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
            	Message message = null;
            	message = (Message) input.readObject();
            	
            	if (message != null) {
            		switch (message.getMsType()) {
                	case CONNECTED:
                		dbPath = message.getDBname();
                		indexPath = message.getResponse();
                		break;
                	
                	case CREATE_DATABASE:
                	case DROP_DATABASE:
                	case CREATE_TABLE:
                	case DROP_TABLE:
                	case CREATE_INDEX:
                	case INSERT_VALUES:
                	case DELETE_VALUES:
                		controller.printResponse(message.getResponse());
                		break;
                		
                	default:
                		System.out.println("Unrecognized message type.");
                		break;
            		}
            	}
            }
        }
        catch (Exception e) {
        	e.printStackTrace();
        }
    }
    
    public static void getPaths() throws IOException {
    	Message message = new Message();
    	message.setMsType(MessageType.CONNECTED);
    	oos.writeObject(message);
    	oos.flush();
    }
    
    public static void sendRequest(Message msg) throws IOException {
    	oos.writeObject(msg);
    	oos.flush();
    }

	public static String getDbPath() {
		return dbPath;
	}

	public static String getIndexPath() {
		return indexPath;
	}
    
}
