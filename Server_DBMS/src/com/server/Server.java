package com.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import message.Message;
import message.MessageType;

public class Server {

	private static final int PORT = 9001;
	private static Path absPath = Paths.get(".").normalize().toAbsolutePath();
	
	public static void main(String[] args) throws IOException {
		System.out.println("Server started.");
		ServerSocket listener = new ServerSocket(PORT);

        try {
            while (true) {
                new Handler(listener.accept()).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            listener.close();
        }
		
	}
	
    private static class Handler extends Thread {
        private String name;
        private Socket socket;
        private ObjectInputStream input;
        private OutputStream os;
        private ObjectOutputStream output;
        private InputStream is;

        public Handler(Socket socket) throws IOException {
            this.socket = socket;
        }

        public void run() {
            try {
                is = socket.getInputStream();
                input = new ObjectInputStream(is);
                os = socket.getOutputStream();
                output = new ObjectOutputStream(os);
                
                while (socket.isConnected()) {
                	
                	Message inputMessage = (Message) input.readObject(); //MESSAGE FROM CLIENT THROUGH SOCKET
                	
                	//statementState -> response for the client
                	int statementState;
                	
                	if (inputMessage != null) {
                    	switch(inputMessage.getMsType()) {
                    	
                    	case CONNECTED:
                    		constructResponse(99);
                    		break;
                    	
                    	case CREATE_DATABASE:
                    		statementState = DDL.createDatabase(inputMessage.getDBname());
                    		break;
                    		
                    	case DROP_DATABASE:
                    		statementState = DDL.dropDatabase(inputMessage.getDBname());
                    		break;
                    		
                    	case CREATE_TABLE:
                    		statementState = DDL.createTable(inputMessage.getDBname(), inputMessage.getTbname(), inputMessage.getColumns());
                    		break;
                    		
                    	case DROP_TABLE:
                    		statementState = DDL.dropTable(inputMessage.getDBname(), inputMessage.getTbname());
                    		break;
                    		
                    	case CREATE_INDEX:
                    		statementState = DDL.createIndex(inputMessage.getDBname(), inputMessage.getTbname(), inputMessage.getColumns().get(0).getName(), inputMessage.getUserGivenName());
                    		break;
                    		
                    	default:
                    		System.out.println("Unrecognized message type.");
                    		break;
                    	}
                	}
                }
                
            } catch (SocketException e){
                System.out.println("User disconnected!");
            } catch (Exception e) {
            	e.printStackTrace();
            }
        }
        
        public void constructResponse(int statementState) throws IOException {
        	
        	Message response = new Message();
        	
        	switch (statementState) {
        	
        	//User connected, sending paths to DB/Indexes
        	case 99:
        		response.setMsType(MessageType.CONNECTED);
        		response.setDBname(absPath + "\\databases\\");
        		response.setResponse(absPath + "\\indexes\\");
        	}
        	
        	output.writeObject(response);
        }
    }

}
