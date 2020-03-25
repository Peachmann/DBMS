package com.client.db;

import java.net.URL;
import java.util.ResourceBundle;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Label;
import javafx.stage.Stage;

public class DeleteCon implements Initializable {
	
	private Stage stage;
	private String tableName, databaseName;
	@FXML private Label databaseLabel, tableLabel;
	
	public DeleteCon(String dbname, String tbname) {
		this.databaseName = dbname;
		this.tableName = tbname;
	}
	
	@Override
	public void initialize(URL arg0, ResourceBundle arg1) {
		databaseLabel.setText(" (" + databaseName + ")");
		tableLabel.setText(tableName);
	}
	
	@FXML
	public void delete() {
		
	}

	public void setStage(Stage stage) {
		this.stage = stage;
	}
	
	@FXML
	public void cancelPopup() {
		stage.close();
	}
}
