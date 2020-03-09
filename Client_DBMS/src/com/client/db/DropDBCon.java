package com.client.db;

import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.ComboBox;
import javafx.stage.Stage;
import message.Message;
import message.MessageType;
import structure.DBStructure;

public class DropDBCon implements Initializable {

	private Stage stage;
	@FXML private ComboBox<String> dbNameCombobox;
	
	@Override
	public void initialize(URL arg0, ResourceBundle arg1) {
		dbNameCombobox.getItems().addAll(DBStructure.getDatabases());
	}
	
	public void setStage(Stage stage) {
		this.stage = stage;
	}

	@FXML
	public void dropDB() throws IOException {
		Message msg = new Message();
		msg.setMsType(MessageType.DROP_DATABASE);
		msg.setDBname(dbNameCombobox.getSelectionModel().getSelectedItem());
		Listener.sendRequest(msg);
		stage.close();
	}
	
	@FXML
	public void cancelPopup() {
		stage.close();
	}
}
