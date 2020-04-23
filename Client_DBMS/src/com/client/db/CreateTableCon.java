package com.client.db;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

import javafx.collections.FXCollections;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.ComboBox;
import javafx.scene.control.TextField;
import javafx.scene.control.ToggleButton;
import javafx.scene.control.ToggleGroup;
import javafx.stage.Stage;
import message.Attribute;
import message.Constraints;
import message.Message;
import message.MessageType;
import structure.DBStructure;

public class CreateTableCon implements Initializable{

	@FXML private ComboBox<String> databaseName, attType, attFKTable, attFKAtt;
	@FXML private TextField tableName, attName;
	@FXML private ToggleButton attPK, attFK, attUnique;
	private ToggleGroup grp;
	private ArrayList<Attribute> list;
	private Stage stage;
	
	@Override
	public void initialize(URL arg0, ResourceBundle arg1) {
		list = new ArrayList<Attribute>();
		databaseName.getItems().addAll(DBStructure.getDatabases());
		attType.setItems(FXCollections.observableArrayList(
				"int", "varchar (50)", "datetime", "float", "bit"));
		grp = new ToggleGroup();
		attPK.setToggleGroup(grp);
		attFK.setToggleGroup(grp);
	}

	@FXML
	public void addAttribute() {
		Attribute newAttr;
		String type = "";
		switch(attType.getSelectionModel().getSelectedItem()) {
		case "int":
			type = "int";
			break;
		case "datetime":
			type = "date";
			break;
		case "varchar (50)":
			type = "varchar50";
			break;
		case "float":
			type = "float";
			break;
		case "bit":
			type = "bit";
			break;
		}
		
		if(attPK.isSelected()) {
			newAttr = new Attribute(attName.getText(), type, Constraints.PRIMARY_KEY);
			newAttr.setIsUnique(true);
		} else if (attFK.isSelected()) {
			newAttr = new Attribute(attName.getText(), type, Constraints.FOREIGN_KEY);
			newAttr.setReference(attFKTable.getSelectionModel().getSelectedItem(), attFKAtt.getSelectionModel().getSelectedItem());
			newAttr.setIsUnique(false);
		} else {
			newAttr = new Attribute(attName.getText(),type);
			if(attUnique.isSelected()) {
				newAttr.setIsUnique(true);
			} else {
				newAttr.setIsUnique(false);
			}
		}
			
		list.add(newAttr);
		clearAll();
	}
	
	@FXML
	public void clearAll() {
		attFKTable.getSelectionModel().clearSelection();
		attFKAtt.getSelectionModel().clearSelection();
		attType.getSelectionModel().clearSelection();
		attName.clear();
		grp.selectToggle(null);
	}
	
	@FXML
	public void setFKTable() {
		attFKTable.getItems().clear();
		attFKTable.getItems().addAll(DBStructure.getTables(databaseName.getSelectionModel().getSelectedItem()));
	}
	
	@FXML
	public void setFKAtt() {
		attFKAtt.getItems().clear();
		List<String> list = DBStructure.getAttributes(databaseName.getSelectionModel().getSelectedItem(), attFKTable.getSelectionModel().getSelectedItem());
		attFKAtt.getItems().addAll(list.stream().map(table -> table.substring(table.indexOf('#') + 1)).collect(Collectors.toList()));
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
