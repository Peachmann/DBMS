package com.client.db;

import java.net.URL;
import java.util.ResourceBundle;

import com.client.login.MainLauncher;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.Cursor;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TreeItem;
import javafx.scene.image.ImageView;
import javafx.scene.layout.BorderPane;

public class DBController implements Initializable {

	@FXML Button minimizeButton, closeButton;
	@FXML ImageView newDB;
	@FXML Label userLabel;
	@FXML BorderPane borderPane;
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
	
	
}
