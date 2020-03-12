package com.client.db;

import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.ComboBox;
import javafx.stage.Stage;
import message.Message;
import message.MessageType;
import structure.DBStructure;

public class DropTableCon implements Initializable {

	private Stage stage;
	@FXML private ComboBox<String> databaseName, tableName;
	
	@Override
	public void initialize(URL arg0, ResourceBundle arg1) {
		databaseName.getItems().addAll(DBStructure.getDatabases());
	}

	public void setStage(Stage stage) {
		this.stage = stage;
	}
	
	@FXML
	public void getTables() {
		tableName.getItems().clear();
		tableName.getItems().addAll(DBStructure.getTables(databaseName.getSelectionModel().getSelectedItem()));
	}
	
	@FXML
	public void dropTable() throws IOException {
		Message msg = new Message();
		msg.setMsType(MessageType.DROP_TABLE);
		msg.setDBname(databaseName.getSelectionModel().getSelectedItem());
		msg.setTbname(tableName.getSelectionModel().getSelectedItem());
		Listener.sendRequest(msg);
		stage.close();
	}
	
	@FXML
	public void cancelPopup() {
		stage.close();
	}
}
