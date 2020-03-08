package com.client.db;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import com.client.login.MainLauncher;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Cursor;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.Tooltip;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;
import javafx.scene.image.ImageView;
import javafx.scene.layout.BorderPane;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.StageStyle;
import structure.DBStructure;

public class DBController implements Initializable {

	@FXML private Button minimizeButton, closeButton;
	@FXML private ImageView newDB, dropDBButton, createTableButton, dropTableButton, createIndexButton, dropIndexButton, refreshButton;
	@FXML private Label userLabel;
	@FXML private BorderPane borderPane;
	@FXML private TreeView<String> treeView;
	@FXML private TextArea responseTextArea;
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
        
        responseTextArea.setText("");
        
        Tooltip.install(newDB, new Tooltip("Create Database"));
        Tooltip.install(dropDBButton, new Tooltip("Drop Database"));
        Tooltip.install(createTableButton, new Tooltip("Create Table"));
        Tooltip.install(dropTableButton, new Tooltip("Drop Table"));
        Tooltip.install(createIndexButton, new Tooltip("Create Index"));
        Tooltip.install(dropIndexButton, new Tooltip("Drop Index"));
        Tooltip.install(refreshButton, new Tooltip("Refresh"));
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
	public void refreshView() throws IOException, InterruptedException {
		try {
			Listener.getPaths();
			Thread.sleep(100);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		DBStructure.setDbPath(Listener.getDbPath());
		DBStructure.setIndexPath(Listener.getIndexPath());
		
		buildViewTree();
	}
	
	@FXML
	public void createDatabase() throws IOException {
		Parent root;
        try {
        	FXMLLoader fxmlLoader = new FXMLLoader(getClass().getClassLoader().getResource("resources/views/createDBView.fxml"));
            root = fxmlLoader.load();
            CreateDBCon con = (CreateDBCon) fxmlLoader.getController();
            Stage stage = new Stage();
            stage.initStyle(StageStyle.UNDECORATED);
            stage.initModality(Modality.APPLICATION_MODAL);
            stage.setScene(new Scene(root, 300, 150));
            con.setStage(stage);
            stage.showAndWait();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	@FXML
	public void dropDatabase() {
		
	}
	
	@FXML
	public void createTable() {
		
	}
	
	@FXML
	public void dropTable() {
		
	}
	
	@FXML
	public void createIndex() {
		
	}
	
	@FXML
	public void dropIndex() {
		
	}
	
	@FXML
	public void minimizeWindow() {
		MainLauncher.getPrimaryStage().setIconified(true);
	}
	
	public void printResponse(String msg) {
		StringBuilder text = new StringBuilder("");
		text.append(responseTextArea.getText());
		text.append(msg);
		text.append("\n");
		responseTextArea.setText(text.toString());
	}
	
	// This is what I tried out
	/*
	 * https://gist.github.com/jewelsea/5174074
	 * https://docs.oracle.com/javafx/2/ui_controls/tree-view.htm
	 * https://www.youtube.com/watch?v=nm8_ZmMiHQA
	 * */
	public void buildViewTree() {
		
		TreeItem<String> root = new TreeItem<String>("Everything");
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
