package com.client.login;

import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;

import com.client.db.DBController;
import com.client.db.Listener;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Cursor;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.Hyperlink;
import javafx.scene.control.TextField;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;
import javafx.stage.WindowEvent;

public class LoginController implements Initializable {
	
	public static DBController con;
	@FXML Pane basePane;
	@FXML TextField userTextField, ipTextField, portTextField;
	@FXML Button loginButton;
	@FXML Hyperlink closeHyper;
	private Scene scene;
	private double xOffset, yOffset;
	private static LoginController instance;
	private Stage stage;
	
	public Stage getStage() {
		return stage;
	}

	//Getting instance to help serialization
    public LoginController() {
        instance = this;
    }
	
    @Override
    public void initialize(URL location, ResourceBundle resources) {

    	//userTextField.setText("");
    	ipTextField.setText("localhost");
    	portTextField.setText("9001");
    	
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
        
        stage = MainLauncher.getPrimaryStage();
    }
    
    @FXML
    public void loginButtonAction() throws IOException {
        String hostname = ipTextField.getText();
        int port = Integer.parseInt(portTextField.getText());
        String username = userTextField.getText();
        
        FXMLLoader fmxlLoader = new FXMLLoader(getClass().getClassLoader().getResource("resources/views/DBView.fxml"));
        Parent window = fmxlLoader.load();
        con = fmxlLoader.<DBController>getController();
        con.setStage(stage);
        new Thread(new Listener(hostname, port, username, con)).start();
        this.scene = new Scene(window);
    }
    
    public void showScene() throws IOException {
        Platform.runLater(() -> {
            Stage stage = (Stage) ipTextField.getScene().getWindow();
            stage.setResizable(false);
            stage.setWidth(1024);
            stage.setHeight(768);

            stage.setOnCloseRequest((WindowEvent e) -> {
                Platform.exit();
                System.exit(0);
            });
            stage.setScene(this.scene);
            stage.centerOnScreen();
            con.setUsernameLabel(userTextField.getText());
        });
    }

    public static LoginController getInstance() {
        return instance;
    }
    
    @FXML
    private void closeLogin() {
    	Platform.exit();
        System.exit(0);
    }
    
    //Login error
    public void showErrorDialog(String message) {
        Platform.runLater(()-> {
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("Warning!");
            alert.setHeaderText(message);
            alert.setContentText("Please check for firewall issues and check if the server is running.");
            alert.showAndWait();
        });
    }
	
}
