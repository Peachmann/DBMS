package com.client.db;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.stage.Stage;
import message.Attribute;
import message.Message;
import message.MessageType;
import structure.DBStructure;

public class InsertCon implements Initializable {

	private Integer i, n, k;
	private Stage stage;
	private String tableName, databaseName;
	@FXML private Label tableLabel, databaseLabel, addedCounter, columnName, columnType;
	@FXML private TextField valueTextfield;
	private List<String> allColumnName, allColumnType, allAtt;
	private ArrayList<Attribute> values;
	
	public InsertCon(String dbname, String tablename) {
		this.databaseName = dbname;
		this.tableName = tablename;
		allAtt = DBStructure.getAttributes(dbname, tablename);
		n = allAtt.size();
		allColumnName = new ArrayList<String>();
		allColumnType = new ArrayList<String>();
		values = new ArrayList<Attribute>();
		
		for(int j = 0; j < n; j++) {
			String[] aux = allAtt.get(j).split("#");
			allColumnName.add(aux[1]);
			allColumnType.add(aux[0]);
		}
		
		//reserving position 0 to store the number of inserts later on
		values.add(new Attribute("a", "b"));
		
		i = 0;
		k = 0;
	}
	
	@Override
	public void initialize(URL arg0, ResourceBundle arg1) {
		tableLabel.setText(tableName);
		databaseLabel.setText(" (" + databaseName + ")");
		addedCounter.setText(k.toString());
		columnName.setText(allColumnName.get(0));
		columnType.setText(allColumnType.get(0));
	}
	
	public void setStage(Stage stage) {
		this.stage = stage;
	}

	@FXML
	public void doneButton() throws IOException {
		//First parameter = number of attributes, second parameter = total inserts
		values.set(0, new Attribute(n.toString(), addedCounter.getText()));
		
		Message msg = new Message();
		msg.setMsType(MessageType.INSERT_VALUES);
		msg.setDBname(databaseName);
		msg.setTbname(tableName);
		msg.setColumns(values);
		Listener.sendRequest(msg);
		stage.close();
	}
	
	@FXML
	public void nextColumn() {
		values.add(new Attribute(columnName.getText(), columnType.getText(), valueTextfield.getText()));
		
		System.out.println(columnType.getText());
		
		if(n-1 == i) {
			k++;
			i = -1;
			addedCounter.setText(k.toString());
		}
		
		i++;
		columnName.setText(allColumnName.get(i));
		columnType.setText(allColumnType.get(i));
		valueTextfield.clear();
	}
	
	@FXML
	public void cancelPopup() {
		stage.close();
	}
	
}
