package com.client.db;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import com.client.login.MainLauncher;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.Cursor;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;
import javafx.scene.image.ImageView;
import javafx.scene.layout.BorderPane;
import structure.DBStructure;

public class DBController implements Initializable {

	@FXML Button minimizeButton, closeButton;
	@FXML ImageView newDB;
	@FXML Label userLabel;
	@FXML BorderPane borderPane;
	@FXML TreeView<String> treeView;
    private double xOffset, yOffset;
	
	@Override
	public void initialize(URL arg0, ResourceBundle arg1) {
        borderPane.setOnMousePressed(event -> {
            xOffset = MainLauncher.getPrimaryStage().getX() - event.getScreenX();
            yOffset = MainLauncher.getPrimaryStage().getY() - event.getScreenY();
            borderPane.setCursor(Cursor.CLOSED_HAND);
        });

        borderPane.setOnMouseDragged(event -> {
            MainLauncher.getPrimaryStage().setX(event.getScreenX() + xOffset);
            MainLauncher.getPrimaryStage().setY(event.getScreenY() + yOffset);

        });

        borderPane.setOnMouseReleased(event -> {
            borderPane.setCursor(Cursor.DEFAULT);
        });
        buildViewTree();
	}

	public void setUsernameLabel(String text) {
		userLabel.setText("Welcome back, " + text + "!");
	}
	
	@FXML
	public void closeWindow() {
        Platform.exit();
        System.exit(0);
	}

	@FXML
	public void createDatabase() {
		//name.getText();
	}
	
	@FXML
	public void minimizeWindow() {
		MainLauncher.getPrimaryStage().setIconified(true);
	}
	
	// This is what I tried out
	/*
	 * https://gist.github.com/jewelsea/5174074
	 * https://docs.oracle.com/javafx/2/ui_controls/tree-view.htm
	 * https://www.youtube.com/watch?v=nm8_ZmMiHQA
	 * */
	public void buildViewTree() {
		
		TreeItem<String> root = new TreeItem<String>("EVERYTHING");
		root.setExpanded(true);
		
		List<ArrayList<String>> structure = DBStructure.getAllDBTables();
		
		for(ArrayList<String> db : structure) {
			
			TreeItem<String> dbname = new TreeItem<String>(db.get(0));
			for(int i = 1; i < db.size(); i++) {
				
				dbname.getChildren().add(new TreeItem<String>(db.get(i)));
			}
			root.getChildren().add(dbname);
		}
		
		treeView.setRoot(root);
	}
	
	
}
