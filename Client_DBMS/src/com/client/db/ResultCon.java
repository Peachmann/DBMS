package com.client.db;

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

public class ResultCon implements Initializable {

	private Stage stage;
	@FXML private TableView<ObservableList<String>> resultTable;
	@FXML private Label selectedRowsLabel;
	private List<String> columnNames, values;
	
	public ResultCon(ArrayList<String> v) {
		resultTable = new TableView<ObservableList<String>>();
		values = v;
		selectedRowsLabel = new Label();
		System.out.println(values);
	}

	public void setStage(Stage stage) {
		this.stage = stage;
	}

	@Override
	public void initialize(URL arg0, ResourceBundle arg1) {
		// Building table layout
		columnNames = new ArrayList<String>();
		columnNames.add("No.");
		columnNames.add("Table");
		columnNames.add("Name");
		columnNames.add("Type");
		columnNames.add("Value");

		for (int i = 0; i < columnNames.size(); i++) {
			final int ii = i;
			TableColumn<ObservableList<String>, String> column = new TableColumn<>(columnNames.get(i));
			column.setCellValueFactory(param -> new ReadOnlyObjectWrapper<>(param.getValue().get(ii)));
			resultTable.getColumns().add(column);
		}
		
		fill();
	}
	
	public void fill() {
		ObservableList<ObservableList<String>> data = FXCollections.observableArrayList();
		int k = 0, counter = 0;
		for (int i = 0; i < values.size(); i++) {
			String[] aux = values.get(i).split("#");
			ObservableList<String> row = FXCollections.observableArrayList();
			row.add(String.valueOf(++counter));
			for (int j = 0; j < aux.length; j++) {
				row.add(aux[j]);
				
				k++;
				if(k == 4) {
					data.add(row);
					System.out.println(row);
					row = FXCollections.observableArrayList();
					row.add(String.valueOf(counter));
					k = 0;
				}
			}
			//counter--;
		}

		resultTable.setItems(data);
		resultTable.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
		selectedRowsLabel.setText(String.valueOf(counter));
	}
	
	@FXML
	public void cancelPopup() {
		stage.close();
	}
}
