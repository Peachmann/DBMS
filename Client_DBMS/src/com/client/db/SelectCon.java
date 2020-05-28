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
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.SelectionMode;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.stage.Stage;
import message.Aggregate;
import message.AggregateType;
import message.JoinOn;
import message.Message;
import message.MessageType;
import message.Operator;
import message.Where;
import structure.DBStructure;

public class SelectCon implements Initializable {

	private Stage stage;
	@FXML private ComboBox<String> databaseName, tableName, columnBox, operatorBox, joinTable1, joinTable2, joinAtt1, joinAtt2, havingColBox, havingOpBox, functionBox, groupbyBox;
	@FXML private TextField compareField, havingCompareField;
	@FXML private TableView<ObservableList<String>> columnTable;
	@FXML private TextArea whereArea, havingArea;
	@FXML private CheckBox asSelectBox, groupbyCheck;
	private List<String> columnNames;
	private ArrayList<Where> whereList;
	private Boolean empty, empty2;
	private ArrayList<JoinOn> joinList;
	private ArrayList<Aggregate> havingAgg, selectAgg;
	
	public SelectCon() {
		columnTable = new TableView<ObservableList<String>>();
		empty = true;
		empty2 = true;
	}
	
	@Override
	public void initialize(URL arg0, ResourceBundle arg1) {
		databaseName.getItems().addAll(DBStructure.getDatabases());
		whereList = new ArrayList<Where>();
		joinList = new ArrayList<JoinOn>();
		havingAgg = selectAgg = new ArrayList<Aggregate>();
		
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
		
		ObservableList<String> aggs = FXCollections.observableArrayList();
		aggs.add("SUM");
		aggs.add("COUNT");
		aggs.add("MIN");
		aggs.add("MAX");
		aggs.add("AVG");
		
		functionBox.setItems(aggs);
		operatorBox.setItems(ops);
		havingOpBox.setItems(ops);
	}
	
	@FXML
	public void addJoin() {
		JoinOn aux = new JoinOn();
		aux.setTable1(joinTable1.getSelectionModel().getSelectedItem());
		aux.setTable2(joinTable2.getSelectionModel().getSelectedItem());
		aux.setAttribute1(joinAtt1.getSelectionModel().getSelectedItem());
		aux.setAttribute2(joinAtt2.getSelectionModel().getSelectedItem());
		joinList.add(aux);
		
		joinTable1.getSelectionModel().clearSelection();
		joinTable2.getSelectionModel().clearSelection();
		joinAtt1.getSelectionModel().clearSelection();
		joinAtt2.getSelectionModel().clearSelection();
	}
	
	@FXML
	public void joinAttRefresh() {
		joinAtt1.getItems().clear();
		joinAtt1.getItems().addAll(DBStructure.getAttributes(databaseName.getSelectionModel().getSelectedItem(), joinTable1.getSelectionModel().getSelectedItem()));
	}
	
	@FXML
	public void joinTableRefresh() {
		joinTable1.getItems().clear();
		joinTable1.getItems().add(tableName.getSelectionModel().getSelectedItem());
		
		for(int i = 0; i < joinList.size(); i++) {
			joinTable1.getItems().add(joinList.get(i).getTable2());
		}
	}
	
	@FXML
	public void joinTable2Refresh() {
		joinTable2.getItems().clear();
		joinTable2.getItems().addAll(DBStructure.getTables(databaseName.getSelectionModel().getSelectedItem()));
	}
	
	@FXML
	public void joinAtt2Refresh() {
		joinAtt2.getItems().clear();
		joinAtt2.getItems().addAll(DBStructure.getAttributes(databaseName.getSelectionModel().getSelectedItem(), joinTable2.getSelectionModel().getSelectedItem()));
	}

	public void setStage(Stage stage) {
		this.stage = stage;
	}
	
	@FXML
	public void resetWhere() {
		whereArea.setText("WHERE");
		whereList.clear();
		empty = true;
	}
	
	@FXML
	public void resetHaving() {
		havingArea.setText("HAVING");
		havingAgg.clear();
		selectAgg.clear();
		empty2 = true;
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
		
		if (groupbyCheck.isSelected()) {
			String [] split = groupbyBox.getSelectionModel().getSelectedItem().split(" - ");
			selectMessage.setGroupBy(split[0] + "#" + split[1]);
		}
		
		System.out.println(selectMessage.getGroupBy());
		
		selectMessage.setHavingAgg(havingAgg);
		selectMessage.setSelectAgg(selectAgg);
		selectMessage.setJoins(joinList);
		selectMessage.setSelectList(selectList);
		Listener.sendRequest(selectMessage);
		
		cancelPopup();
	}
	
	@FXML
	public void getAllValues() {
		try {
			Message msg = new Message();
			msg.setMsType(MessageType.GET_ALL_VALUES);
			msg.setDBname(databaseName.getSelectionModel().getSelectedItem());
			msg.setTbname(tableName.getSelectionModel().getSelectedItem());
			Listener.sendRequest(msg);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			for (int i = 0; i < joinList.size(); i++) {
				Message msg = new Message();
				msg.setMsType(MessageType.GET_ALL_VALUES);
				msg.setDBname(databaseName.getSelectionModel().getSelectedItem());
				msg.setTbname(joinList.get(i).getTable2());
				Listener.sendRequest(msg);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void refreshValues(String tbName, ArrayList<String> v) {
		System.out.println(tbName);
		for (int i = 0; i < v.size(); i++) {
			ObservableList<String> row = FXCollections.observableArrayList();
			row.add(tbName);
			row.add(v.get(i));
			columnTable.getItems().add(row);
		}
		
		columnTable.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
	}
	
	@FXML
	public void selectAll() {
		columnTable.getSelectionModel().selectAll();
	}
	
	@FXML
	public void getTables() {
		tableName.getItems().clear();
		tableName.getItems().addAll(DBStructure.getTables(databaseName.getSelectionModel().getSelectedItem()));
	}
	
	@FXML
	public void addWhere() {
		Where aux = new Where();
		String opaux = "";
		
		String [] helper = columnBox.getSelectionModel().getSelectedItem().split(" - ");
		aux.setField1(helper[0] + "#" + helper[1]);
		aux.setField2(compareField.getText());
		
		switch (operatorBox.getSelectionModel().getSelectedItem()) {
		case ">":
			opaux = ">";
			aux.setOp(Operator.GT);
			break;

		case "<":
			opaux = "<";
			aux.setOp(Operator.LT);
			break;
			
		case ">=":
			opaux = ">=";
			aux.setOp(Operator.GTE);
			break;
			
		case "<=":
			opaux = "<=";
			aux.setOp(Operator.LTE);
			break;
			
		case "=":
			opaux = "=";
			aux.setOp(Operator.EQ);
			break;
			
		case "!=":
			opaux = "!=";
			aux.setOp(Operator.NEQ);
			break;
			
		default:
			break;
		}
		
		whereList.add(aux);
		
		columnBox.getSelectionModel().clearSelection();
		operatorBox.getSelectionModel().clearSelection();
		compareField.setText("");
		
		if (empty == true)
			whereArea.setText(whereArea.getText() + " " + aux.getField1() + " " + opaux + " " + aux.getField2());
		else
			whereArea.setText(whereArea.getText() + " AND " + aux.getField1() + " " + opaux + " " + aux.getField2());
		
		empty = false;
	}
	
	public void addHaving() {
		
		Aggregate aux = new Aggregate();
		
		if (asSelectBox.isSelected()) {
			aux.setIsSelect(true);
		}
		
		String opaux = "", aggaux = "";
		
		String [] helper = havingColBox.getSelectionModel().getSelectedItem().split(" - ");
		aux.setColumnname(helper[0] + "#" + helper[1]);
		aux.setComparevalue(havingCompareField.getText());
		
		switch (havingOpBox.getSelectionModel().getSelectedItem()) {
		case ">":
			opaux = ">";
			aux.setOp(Operator.GT);
			break;

		case "<":
			opaux = "<";
			aux.setOp(Operator.LT);
			break;
			
		case ">=":
			opaux = ">=";
			aux.setOp(Operator.GTE);
			break;
			
		case "<=":
			opaux = "<=";
			aux.setOp(Operator.LTE);
			break;
			
		case "=":
			opaux = "=";
			aux.setOp(Operator.EQ);
			break;
			
		case "!=":
			opaux = "!=";
			aux.setOp(Operator.NEQ);
			break;
			
		default:
			break;
		}
		
		switch (functionBox.getSelectionModel().getSelectedItem()) {
		case "SUM":
			aggaux = "SUM";
			aux.setType(AggregateType.SUM);
			break;

		case "COUNT":
			aggaux = "COUNT";
			aux.setType(AggregateType.COUNT);
			break;
			
		case "MIN":
			aggaux = "MIN";
			aux.setType(AggregateType.MIN);
			break;
			
		case "MAX":
			aggaux = "MAX";
			aux.setType(AggregateType.MAX);
			break;
			
		case "AVG":
			aggaux = "AVG";
			aux.setType(AggregateType.AVG);
			break;
			
		default:
			break;
		}
		
		System.out.println(aggaux);
		
		if (!aux.getIsSelect()) {
			if (empty2 == true)
				havingArea.setText(havingArea.getText() + " " + aggaux + "(" + aux.getColumnname() + ") " + opaux + " " + aux.getComparevalue());
			else
				havingArea.setText(havingArea.getText() + " AND " + aggaux + "(" + aux.getColumnname() + ") " + opaux + " " + aux.getComparevalue());
			
			empty2 = false;
			havingAgg.add(aux);
		} else {
			selectAgg.add(aux);
		}
		
		havingColBox.getSelectionModel().clearSelection();
		havingOpBox.getSelectionModel().clearSelection();
		functionBox.getSelectionModel().clearSelection();
		havingCompareField.setText("");
	}
	
	public void fillGroupby() {
		groupbyBox.getSelectionModel().clearSelection();
		groupbyBox.getItems().clear();
		
		ObservableList<ObservableList<String>> allColumns = columnTable.getItems();
		for(ObservableList<String> row : allColumns) {
			groupbyBox.getItems().add(row.get(0) + " - " + row.get(1));
		}
	}
	
	public void fillHaving() {
		havingColBox.getSelectionModel().clearSelection();
		havingColBox.getItems().clear();
		
		ObservableList<ObservableList<String>> allColumns = columnTable.getItems();
		for(ObservableList<String> row : allColumns) {
			havingColBox.getItems().add(row.get(0) + " - " + row.get(1));
		}
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
