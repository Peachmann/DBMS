package com.client.db;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Label;
import javafx.scene.control.SelectionMode;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.stage.Stage;
import message.Attribute;
import message.Message;
import message.MessageType;
import structure.DBStructure;

public class DeleteCon implements Initializable {

	@FXML private TableView<ObservableList<String>> valueTable;
	private Stage stage;
	private String tableName, databaseName;
	@FXML private Label databaseLabel, tableLabel;
	private List<String> columnNames, colName, colType;
	private ArrayList<Attribute> deleted;

	public DeleteCon(String dbname, String tbname) {
		this.databaseName = dbname;
		this.tableName = tbname;
		valueTable = new TableView<ObservableList<String>>();
		deleted = new ArrayList<Attribute>();
	}

	@Override
	public void initialize(URL arg0, ResourceBundle arg1) {
		databaseLabel.setText(" (" + databaseName + ")");
		tableLabel.setText(tableName);

		// Building table layout
		List<String> data1 = DBStructure.getAttributes(databaseName, tableName);
		columnNames = new ArrayList<String>();
		colName = new ArrayList<String>();
		colType = new ArrayList<String>();
		for (int i = 0; i < data1.size(); i++) {
			String[] aux = data1.get(i).split("#");
			colName.add(aux[1]);
			colType.add(aux[0]);
			columnNames.add(aux[1] + " (" + aux[0] + ")");
		}
		
		System.out.println(columnNames);
		for (int i = 0; i < columnNames.size(); i++) {
			final int ii = i;
			TableColumn<ObservableList<String>, String> column = new TableColumn<>(columnNames.get(i));
			column.setCellValueFactory(param -> new ReadOnlyObjectWrapper<>(param.getValue().get(ii)));
			valueTable.getColumns().add(column);
		}
		
		// Getting values
		try {
			Message msg = new Message();
			msg.setMsType(MessageType.GET_VALUES);
			msg.setDBname(databaseName);
			msg.setTbname(tableName);
			Listener.sendRequest(msg);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@FXML
	public void delete() {
		ObservableList<ObservableList<String>> selections = FXCollections.observableArrayList();
		selections = valueTable.getSelectionModel().getSelectedItems();
		
		for(int i = 0; i < selections.size(); i++) {
			for(int j = 0; j < selections.get(i).size(); j++) {
				System.out.println(colName.get(j) + " " + colType.get(j) + " " + selections.get(i).get(j));
				deleted.add(new Attribute(colName.get(j), colType.get(j), selections.get(i).get(j)));
			}
		}
		
		try {
			Message msg = new Message();
			msg.setMsType(MessageType.DELETE_VALUES);
			msg.setDBname(databaseName);
			msg.setTbname(tableName);
			msg.setColumns(deleted);
			Listener.sendRequest(msg);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void setStage(Stage stage) {
		this.stage = stage;
	}

	@FXML
	public void cancelPopup() {
		stage.close();
	}

	public void initValues(ArrayList<String> v) {		
		ObservableList<ObservableList<String>> data = FXCollections.observableArrayList();
		for (int i = 0; i < v.size(); i++) {
			String[] aux = v.get(i).split("#");
			ObservableList<String> row = FXCollections.observableArrayList();
			row.add(aux[0]);
			for (int j = 3; j < aux.length; j += 3) {
				row.add(aux[j]);
			}
			System.out.println(row);
			data.add(row);
		}

		valueTable.setItems(data);
		valueTable.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
		System.out.println(v);

	}
}
