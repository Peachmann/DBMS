package com.client.db;

import java.net.URL;
import java.util.ResourceBundle;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.image.ImageView;

public class DBController implements Initializable {

	@FXML Button minimizeButton, closeButton;
	@FXML ImageView newDB;
	@FXML Label userLabel;
	
	@Override
	public void initialize(URL arg0, ResourceBundle arg1) {
		//
	}

	public void setUsernameLabel(String text) {
		userLabel.setText("Welcome back, " + text + "!");
	}

}
