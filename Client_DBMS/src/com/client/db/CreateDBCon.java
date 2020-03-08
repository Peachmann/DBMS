package com.client.db;

import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.TextField;
import javafx.stage.Stage;
import message.Message;
import message.MessageType;

public class CreateDBCon implements Initializable {

	private Stage stage;
	@FXML private TextField dbNameTextfield;
	
	@Override
	public void initialize(URL arg0, ResourceBundle arg1) {
		//
	}
	
	public void setStage(Stage stage) {
		this.stage = stage;
	}

	@FXML
	public void createDB() throws IOException {
		Message msg = new Message();
		msg.setMsType(MessageType.CREATE_DATABASE);
		System.out.println(dbNameTextfield.getText());
		msg.setDBname(dbNameTextfield.getText());
		Listener.sendRequest(msg);
		stage.close();
	}
	
	@FXML
	public void cancelPopup() {
		stage.close();
	}
	

}
