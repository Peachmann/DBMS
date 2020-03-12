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
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.Tooltip;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;
import javafx.scene.image.Image;
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
    private Stage stage, stage1;
	
	public void setStage(Stage stage) {
		this.stage = stage;
	}

	@Override
	public void initialize(URL arg0, ResourceBundle arg1) {
		
        borderPane.setOnMousePressed(event -> {
            xOffset = stage.getX() - event.getScreenX();
            yOffset = stage.getY() - event.getScreenY();
            borderPane.setCursor(Cursor.CLOSED_HAND);
        });

        borderPane.setOnMouseDragged(event -> {
        	stage.setX(event.getScreenX() + xOffset);
        	stage.setY(event.getScreenY() + yOffset);
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
        
        stage1 = new Stage();
        stage1.initStyle(StageStyle.UNDECORATED);
        stage1.initModality(Modality.APPLICATION_MODAL);
	}

	public void setUsernameLabel(String text) {
		userLabel.setText(" Welcome back, " + text + "!");
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
	public void createDatabase() throws IOException, InterruptedException {
		Parent root;
        try {
        	FXMLLoader fxmlLoader = new FXMLLoader(getClass().getClassLoader().getResource("resources/views/createDBView.fxml"));
            root = fxmlLoader.load();
            CreateDBCon con = (CreateDBCon) fxmlLoader.getController();
            stage1.setScene(new Scene(root, 300, 150));
            con.setStage(stage1);
            stage1.showAndWait();
            refreshView();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	@FXML
	public void dropDatabase() throws InterruptedException {
		Parent root;
        try {
        	FXMLLoader fxmlLoader = new FXMLLoader(getClass().getClassLoader().getResource("resources/views/DropDBView.fxml"));
            root = fxmlLoader.load();
            DropDBCon con = (DropDBCon) fxmlLoader.getController();
            stage1.setScene(new Scene(root, 300, 150));
            con.setStage(stage1);
            stage1.showAndWait();
            refreshView();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	@FXML
	public void createTable() throws InterruptedException {
		Parent root;
        try {
        	FXMLLoader fxmlLoader = new FXMLLoader(getClass().getClassLoader().getResource("resources/views/CreateTableView.fxml"));
            root = fxmlLoader.load();
            CreateTableCon con = (CreateTableCon) fxmlLoader.getController();
            stage1.setScene(new Scene(root, 600, 400));
            con.setStage(stage1);
            stage1.showAndWait();
            refreshView();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
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
	
	public void buildViewTree() {
		
		Image dbIcon = new Image(DBController.class.getResourceAsStream("../../../resources/jpg/dbicon.png"),18,18,true,true);
		Image tbIcon = new Image(DBController.class.getResourceAsStream("../../../resources/jpg/tbicon.png"),18,18,true,true);
		Image globe = new Image(DBController.class.getResourceAsStream("../../../resources/jpg/globe.png"),18,18,true,true);
		
		TreeItem<String> root = new TreeItem<String>("Everything",new ImageView(globe));
		root.setExpanded(true);
		
		List<ArrayList<String>> structure = DBStructure.getAllDBTables();
		
		for(ArrayList<String> db : structure) {
			
			TreeItem<String> dbname = new TreeItem<String>(db.get(0),new ImageView(dbIcon));
			for(int i = 1; i < db.size(); i++) {
				dbname.getChildren().add(new TreeItem<String>(db.get(i),new ImageView(tbIcon)));
			}
			root.getChildren().add(dbname);
		}
		
		treeView.setRoot(root);
	}
	
	
}
