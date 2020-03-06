package com.client.login;

import java.net.URL;
import java.util.ResourceBundle;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.Cursor;
import javafx.scene.control.Button;
import javafx.scene.control.Hyperlink;
import javafx.scene.control.TextField;
import javafx.scene.layout.Pane;

public class LoginController implements Initializable {
	
	@FXML Pane basePane;
	@FXML TextField userTextField, ipTextField, portTextField;
	@FXML Button loginButton;
	@FXML Hyperlink closeHyper;
	private double xOffset, yOffset;
	
    @Override
    public void initialize(URL location, ResourceBundle resources) {

    	//userTextField.setText("");
    	ipTextField.setText("localhost");
    	portTextField.setText("6868");
    	
        // Drag and Drop method
        basePane.setOnMousePressed(event -> {
            xOffset = MainLauncher.getPrimaryStage().getX() - event.getScreenX();
            yOffset = MainLauncher.getPrimaryStage().getY() - event.getScreenY();
            basePane.setCursor(Cursor.CLOSED_HAND);
        });

        basePane.setOnMouseDragged(event -> {
            MainLauncher.getPrimaryStage().setX(event.getScreenX() + xOffset);
            MainLauncher.getPrimaryStage().setY(event.getScreenY() + yOffset);

        });

        basePane.setOnMouseReleased(event -> {
        	basePane.setCursor(Cursor.DEFAULT);
        });
    }
    
    @FXML
    public void loginButtonAction() {
        String hostname = ipTextField.getText();
        int port = Integer.parseInt(portTextField.getText());
        String username = userTextField.getText();
        
        System.out.println(port);
    }
    
    @FXML
    private void closeLogin() {
    	Platform.exit();
        System.exit(0);
    }
	
}
