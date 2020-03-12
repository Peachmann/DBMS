package com.client.db;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.ResourceBundle;

import javafx.collections.FXCollections;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.ComboBox;
import javafx.scene.control.RadioButton;
import javafx.scene.control.TextField;
import javafx.stage.Stage;
import message.Attribute;
import message.Message;
import message.MessageType;
import structure.DBStructure;

public class CreateTableCon implements Initializable{

	@FXML private ComboBox<String> databaseName, attType, attFKTable, attFKAtt;
	@FXML private TextField tableName, attName, attFKName;
	@FXML private RadioButton attPK;
	private ArrayList<Attribute> list;
	private Stage stage;
	
	@Override
	public void initialize(URL arg0, ResourceBundle arg1) {
		list = new ArrayList<Attribute>();
		databaseName.getItems().addAll(DBStructure.getDatabases());
		attType.setItems(FXCollections.observableArrayList(
				"int", "varchar (50)", "datetime"));
	}

	@FXML
	public void addAttribute() {
		/**
		 * itt csak kiszopja az adatokat a formból és beteszi a listbe
		 * amit az elõbb már inicializáltam
		 * az OK elküldi az üzenetet
		 */
	}
	
	@FXML
	public void clearAll() {
		attFKTable.getSelectionModel().clearSelection();
		attFKAtt.getSelectionModel().clearSelection();
		attType.getSelectionModel().clearSelection();
		attName.clear();
		tableName.clear();
		attFKName.clear();
	}
	
	@FXML
	public void setFKTable() {
		attFKTable.getItems().clear();
		attFKTable.getItems().addAll(DBStructure.getTables(databaseName.getSelectionModel().getSelectedItem()));
	}
	
	@FXML
	public void setFKAtt() {
		attFKAtt.getItems().clear();
		attFKAtt.getItems().addAll(DBStructure.getAttributes(databaseName.getSelectionModel().getSelectedItem(), attFKTable.getSelectionModel().getSelectedItem()));
	}
	
	@FXML
	public void createTable() throws IOException {
		Message msg = new Message();
		msg.setMsType(MessageType.CREATE_TABLE);
		msg.setDBname(databaseName.getSelectionModel().getSelectedItem());
		msg.setTbname(tableName.getText());
		msg.setColumns(list);
		Listener.sendRequest(msg);
		stage.close();
	}
	
	@FXML
	public void cancelPopup() {
		stage.close();
	}
	
	public void setStage(Stage stage) {
		this.stage = stage;
	}
	
}
