package com.client.db;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.ResourceBundle;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.ComboBox;
import javafx.scene.control.TextField;
import javafx.stage.Stage;
import message.Attribute;
import message.Message;
import message.MessageType;
import structure.DBStructure;

public class CreateIndexCon implements Initializable {

	private Stage stage;
	@FXML private ComboBox<String> databaseName, tableName, attName;
	@FXML private TextField indexName;
	private ArrayList<Attribute> attributeName;
	
	@Override
	public void initialize(URL arg0, ResourceBundle arg1) {
		databaseName.getItems().addAll(DBStructure.getDatabases());
		attributeName = new ArrayList<Attribute>();
	}
	
	@FXML
	public void createIndex() throws IOException {
		Message msg = new Message();
		msg.setMsType(MessageType.CREATE_INDEX);
		msg.setDBname(databaseName.getSelectionModel().getSelectedItem());
		msg.setTbname(tableName.getSelectionModel().getSelectedItem());
		String[] aux = attName.getSelectionModel().getSelectedItem().split("#");
		attributeName.add(new Attribute(aux[1], aux[0]));
		msg.setColumns(attributeName);
		msg.setUserGivenName(indexName.getText());
		Listener.sendRequest(msg);
		stage.close();
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
	public void getAtt() {
		attName.getItems().clear();
		attName.getItems().addAll(DBStructure.getAttributes(databaseName.getSelectionModel().getSelectedItem(), tableName.getSelectionModel().getSelectedItem()));
	}
	
	@FXML
	public void cancelPopup() {
		stage.close();
	}
}
