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
import javafx.scene.control.ComboBox;
import javafx.scene.control.RadioButton;
import javafx.scene.control.SelectionMode;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.ToggleGroup;
import javafx.stage.Stage;
import message.Message;
import message.MessageType;
import message.Operator;
import message.Where;
import structure.DBStructure;

public class SelectCon implements Initializable {

	private Stage stage;
	@FXML private ComboBox<String> databaseName, tableName, joinTables, columnBox, operatorBox;
	@FXML private TextField compareField;
	@FXML private TableView<ObservableList<String>> columnTable;
	@FXML private RadioButton joinType1, joinType2;
	private ToggleGroup buttonGroup;
	private String currentTable;
	private List<String> columnNames;
	private ArrayList<Where> whereList;
	
	public SelectCon() {
		columnTable = new TableView<ObservableList<String>>();
	}
	
	@Override
	public void initialize(URL arg0, ResourceBundle arg1) {
		databaseName.getItems().addAll(DBStructure.getDatabases());
		whereList = new ArrayList<Where>();
		buttonGroup = new ToggleGroup();
		joinType1.setToggleGroup(buttonGroup);
		joinType2.setToggleGroup(buttonGroup);
		
		List<String> data1 = new ArrayList<String>();
		data1.add("From Table");
		data1.add("Column Name");
		columnNames = new ArrayList<String>();
		for (int i = 0; i < data1.size(); i++) {
			columnNames.add(data1.get(i));
		}
		
		System.out.println(columnNames);
		for (int i = 0; i < columnNames.size(); i++) {
			final int ii = i;
			TableColumn<ObservableList<String>, String> column = new TableColumn<>(columnNames.get(i));
			column.setCellValueFactory(param -> new ReadOnlyObjectWrapper<>(param.getValue().get(ii)));
			columnTable.getColumns().add(column);
		}
		
		ObservableList<String> ops = FXCollections.observableArrayList();
		ops.add(">");
		ops.add("<");
		ops.add(">=");
		ops.add("<=");
		ops.add("=");
		ops.add("!=");
		operatorBox.setItems(ops);
	}

	public void setStage(Stage stage) {
		this.stage = stage;
	}
	
	@FXML
	public void select() throws IOException {
		
		Message selectMessage = new Message();
		selectMessage.setWhereList(whereList);
		selectMessage.setDBname(databaseName.getSelectionModel().getSelectedItem());
		selectMessage.setMsType(MessageType.SELECT);
		
		// Loop to fill select list when implementing joins -> Format: "table#columnName"
		ArrayList<String> selectList = new ArrayList<String>();
		for(ObservableList<String> row : columnTable.getSelectionModel().getSelectedItems()) {
			selectList.add(row.get(0) + "#" + row.get(1));
		}
		
		selectMessage.setSelectList(selectList);
		Listener.sendRequest(selectMessage);
	}
	
	@FXML
	public void getAllValues() {
		try {
			Message msg = new Message();
			msg.setMsType(MessageType.GET_ALL_VALUES);
			msg.setDBname(databaseName.getSelectionModel().getSelectedItem());
			msg.setTbname(tableName.getSelectionModel().getSelectedItem());
			currentTable = tableName.getSelectionModel().getSelectedItem(); /////// edit to make it loop-able for joins
			Listener.sendRequest(msg);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void refreshValues(ArrayList<String> v) {
		for (int i = 0; i < v.size(); i++) {
			ObservableList<String> row = FXCollections.observableArrayList();
			row.add(currentTable);
			row.add(v.get(i));
			columnTable.getItems().add(row);
		}
		
		columnTable.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
	}
	
	@FXML
	public void selectAll() {
		columnTable.getSelectionModel().selectAll();
		System.out.println(columnTable.getSelectionModel().getSelectedItems());
	}
	
	@FXML
	public void getTables() {
		tableName.getItems().clear();
		tableName.getItems().addAll(DBStructure.getTables(databaseName.getSelectionModel().getSelectedItem()));
	}
	
	@FXML
	public void addWhere() {
		Where aux = new Where();
		
		aux.setField1(columnBox.getSelectionModel().getSelectedItem());
		aux.setField2(compareField.getText());
		
		switch (operatorBox.getSelectionModel().getSelectedItem()) {
		case ">":
			aux.setOp(Operator.GT);
			break;

		case "<":
			aux.setOp(Operator.LT);
			break;
			
		case ">=":
			aux.setOp(Operator.GTE);
			break;
			
		case "<=":
			aux.setOp(Operator.LTE);
			break;
			
		case "=":
			aux.setOp(Operator.EQ);
			break;
			
		case "!=":
			aux.setOp(Operator.NEQ);
			break;
			
		default:
			break;
		}
		
		whereList.add(aux);
		
		columnBox.getSelectionModel().clearSelection();
		operatorBox.getSelectionModel().clearSelection();
		compareField.setText("");
	}
	
	@FXML
	public void fillBox() {
		columnBox.getSelectionModel().clearSelection();
		columnBox.getItems().clear();
		ObservableList<ObservableList<String>> allColumns = columnTable.getItems();
		
		for(ObservableList<String> row : allColumns) {
			columnBox.getItems().add(row.get(0) + " - " + row.get(1));
		}
	}
	
	@FXML
	public void cancelPopup() {
		stage.close();
	}
}
