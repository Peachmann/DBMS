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

import message.Message;
import message.MessageType;
import mongo.MongoDBBridge;

public class Server {

	private static final int PORT = 9001;
	private static Path absPath = Paths.get(".").normalize().toAbsolutePath();
	private static MongoDBBridge mongo = MongoDBBridge.getInstance(); 

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

					Message inputMessage = (Message) input.readObject(); // MESSAGE FROM CLIENT THROUGH SOCKET

					// statementState -> response for the client
					int statementState;

					if (inputMessage != null) {
						switch (inputMessage.getMsType()) {

						case CONNECTED:
							constructResponse(99, inputMessage);
							break;

						case CREATE_DATABASE:
							statementState = DDL.createDatabase(inputMessage.getDBname());
							switch (statementState) {

							case 0:
								constructResponse(1, inputMessage);
								break;
							case -1:
								constructResponse(2, inputMessage);
								break;
							case -2:
								constructResponse(21, inputMessage);
								break;
							case -5:
								constructResponse(3, inputMessage);
								break;
							}
							break;

						case DROP_DATABASE:
							statementState = DDL.dropDatabase(inputMessage.getDBname());
							switch (statementState) {

							case 0:
								mongo.mdbDropDB(inputMessage.getDBname());
								constructResponse(4, inputMessage);
								break;
							case -1:
								constructResponse(5, inputMessage);
								break;
							case -2:
								constructResponse(6, inputMessage);
								break;
							}
							break;

						case CREATE_TABLE:
							statementState = DDL.createTable(inputMessage.getDBname(), inputMessage.getTbname(),
									inputMessage.getColumns());
							switch (statementState) {

							case 0:
								mongo.mdbCreateTable(inputMessage.getDBname(), inputMessage.getTbname());
								constructResponse(7, inputMessage);
								break;
							case -2:
								constructResponse(8, inputMessage);
								break;
							case -3:
								constructResponse(22, inputMessage);
								break;
							case -5:
								constructResponse(9, inputMessage);
								break;
							case -7:
								constructResponse(10, inputMessage);
								break;
							case -10:
								constructResponse(11, inputMessage);
								break;
							case -1:
								constructResponse(12, inputMessage);
								break;
							}
							break;

						case DROP_TABLE:
							statementState = DDL.dropTable(inputMessage.getDBname(), inputMessage.getTbname());
							switch (statementState) {

							case 0:
								mongo.mdbDropTable(inputMessage.getDBname(), inputMessage.getTbname());
								constructResponse(13, inputMessage);
								break;
							case -2:
								constructResponse(14, inputMessage);
								break;
							case -5:
								constructResponse(15, inputMessage);
								break;
							case -1:
								constructResponse(16, inputMessage);
								break;
							}
							break;

						case CREATE_INDEX:
							statementState = DDL.createIndex(inputMessage.getDBname(), inputMessage.getTbname(),
									inputMessage.getColumns().get(0).getName(), inputMessage.getUserGivenName());
							switch (statementState) {

							case 0:
								constructResponse(17, inputMessage);
								break;
							case -5:
								constructResponse(18, inputMessage);
								break;
							case -4:
								constructResponse(19, inputMessage);
								break;
							case -1:
								constructResponse(20, inputMessage);
								break;
							}
							break;
						
						case INSERT_VALUES:
							statementState = DML.insertValues(inputMessage.getDBname(), inputMessage.getTbname(),
									inputMessage.getColumns().get(0).getName(), inputMessage.getColumns().get(0).getType(), inputMessage.getColumns());
							switch (statementState) {
							case 0:
								constructResponse(23, inputMessage);
								break;
							case -1:
								constructResponse(24, inputMessage);
								break;
							}
						
						case DELETE_VALUES:
							statementState = DML.deleteValues();
							switch (statementState) {
							case 0:
								constructResponse(25, inputMessage);
								break;
							case -1:
								constructResponse(26, inputMessage);
								break;
							}
						}
					}
				}
			} catch (SocketException e) {
				System.out.println("User disconnected!");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void constructResponse(int statementState, Message inputMessage) throws IOException {

			Message response = new Message();

			switch (statementState) {

			case 1:
				response.setMsType(MessageType.CREATE_DATABASE);
				response.setResponse("Database " + inputMessage.getDBname() + " created successfully.");
				break;

			case 2:
				response.setMsType(MessageType.CREATE_DATABASE);
				response.setResponse("An error occured, could not create " + inputMessage.getDBname() + " database.");
				break;

			case 3:
				response.setMsType(MessageType.CREATE_DATABASE);
				response.setResponse("Database " + inputMessage.getDBname() + " already exists!");
				break;

			case 4:
				response.setMsType(MessageType.DROP_DATABASE);
				response.setResponse("Database " + inputMessage.getDBname() + " deleted successfully.");
				break;

			case 5:
				response.setMsType(MessageType.DROP_DATABASE);
				response.setResponse("Database " + inputMessage.getDBname() + " does not exist.");
				break;

			case 6:
				response.setMsType(MessageType.DROP_DATABASE);
				response.setResponse("An error occured, could not drop " + inputMessage.getDBname() + " database.");
				break;

			case 7:
				response.setMsType(MessageType.CREATE_TABLE);
				response.setResponse("Table " + inputMessage.getTbname() + " created successfully in database "
						+ inputMessage.getDBname() + ".");
				break;

			case 8:
				response.setMsType(MessageType.CREATE_TABLE);
				response.setResponse("Table " + inputMessage.getTbname() + " already exists in database "
						+ inputMessage.getDBname() + ".");
				break;

			case 9:
				response.setMsType(MessageType.CREATE_TABLE);
				response.setResponse("Could not create table " + inputMessage.getTbname()
						+ ", the number of Primary Keys in one table must be 1!");
				break;

			case 10:
				response.setMsType(MessageType.CREATE_TABLE);
				response.setResponse("Could not create table " + inputMessage.getTbname()
						+ ", because Foreign Key reference does not exists or the type differs.");
				break;

			case 11:
				response.setMsType(MessageType.CREATE_TABLE);
				response.setResponse(
						"Could not create table " + inputMessage.getTbname() + ", because of unsupported field types!");
				break;

			case 12:
				response.setMsType(MessageType.CREATE_TABLE);
				response.setResponse("An error occured, could not create " + inputMessage.getTbname() + " in database "
						+ inputMessage.getDBname() + ".");
				break;

			case 13:
				response.setMsType(MessageType.DROP_TABLE);
				response.setResponse("Table " + inputMessage.getTbname() + " successfully deleted from database "
						+ inputMessage.getDBname() + ".");
				break;

			case 14:
				response.setMsType(MessageType.DROP_TABLE);
				response.setResponse("Table " + inputMessage.getTbname() + " does not exist in database "
						+ inputMessage.getDBname() + ".");
				break;

			case 15:
				response.setMsType(MessageType.DROP_TABLE);
				response.setResponse("Could not drop table " + inputMessage.getTbname() + " from database "
						+ inputMessage.getDBname() + " because other tables have Foreign Key references.");
				break;

			case 16:
				response.setMsType(MessageType.DROP_TABLE);
				response.setResponse("An error occured, could not drop table " + inputMessage.getTbname()
						+ " from database " + inputMessage.getDBname() + ".");
				break;

			case 17:
				response.setMsType(MessageType.CREATE_INDEX);
				response.setResponse(
						"Index successfully created on " + inputMessage.getColumns().get(0).getName() + " column.");
				break;

			case 18:
				response.setMsType(MessageType.CREATE_INDEX);
				response.setResponse(
						"Index on column " + inputMessage.getColumns().get(0).getName() + " already exists.");
				break;

			case 19:
				response.setMsType(MessageType.CREATE_INDEX);
				response.setResponse("Could not create index on column " + inputMessage.getColumns().get(0).getName()
						+ ", name can not contain #_. /\\ characters");
				break;

			case 20:
				response.setMsType(MessageType.CREATE_INDEX);
				response.setResponse("An error occured, could not create index on column "
						+ inputMessage.getColumns().get(0).getName() + ".");
				break;
				
			case 21:
				response.setMsType(MessageType.CREATE_DATABASE);
				response.setResponse("Could not create database " + inputMessage.getDBname()
						+ ", database name can not contain #_. /\\ characters");
				break;
				
			case 22:
				response.setMsType(MessageType.CREATE_TABLE);
				response.setResponse("Could not create table " + inputMessage.getTbname() + " in database " + inputMessage.getDBname()
						+ ", table name can not contain #_. /\\ characters");
				break;
				
			case 23:
				response.setMsType(MessageType.INSERT_VALUES);
				response.setResponse("Values inserted successfully in table " + inputMessage.getTbname() + " (" + inputMessage.getDBname() + ").");
				break;
				
			case 24:
				response.setMsType(MessageType.INSERT_VALUES);
				response.setResponse("Values not inserted! Check input format!");
				break;

			case 99:
				response.setMsType(MessageType.CONNECTED);
				response.setDBname(absPath + "\\databases\\");
				response.setResponse(absPath + "\\indexes\\");
				break;
			}

			output.writeObject(response);
		}
	}

}
