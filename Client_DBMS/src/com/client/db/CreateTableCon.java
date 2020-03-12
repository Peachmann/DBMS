package com.client.db;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.ResourceBundle;

import javafx.collections.FXCollections;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.ComboBox;
import javafx.scene.control.RadioButton;
import javafx.scene.control.TextField;
import javafx.stage.Stage;
import message.Attribute;
import message.Constraints;
import message.Message;
import message.MessageType;
import structure.DBStructure;

public class CreateTableCon implements Initializable{

	@FXML private ComboBox<String> databaseName, attType, attFKTable, attFKAtt;
	@FXML private TextField tableName, attName, attFKName;
	@FXML private RadioButton attPK;
	private ArrayList<Attribute> list;
	private Stage stage;
	
	@Override
	public void initialize(URL arg0, ResourceBundle arg1) {
		list = new ArrayList<Attribute>();
		databaseName.getItems().addAll(DBStructure.getDatabases());
		attType.setItems(FXCollections.observableArrayList(
				"int", "varchar (50)", "datetime", "float", "bit"));
	}

	@FXML
	public void addAttribute() {
		/**
		 * itt csak kiszopja az adatokat a formb�l �s beteszi a listbe
		 * amit az el�bb m�r inicializ�ltam
		 * az OK elk�ldi az �zenetet
		 */
		/*
		 * Nam�rmost, ez j�, de nem �rtem pontosan hogy gondoltad itt el teljesen a strukturat, egyel�re nem irok �t
		 * semmit, csak ezt a r�szletet irtam �s adok p�r javaslatot. Kezdj�k az alapokkal, nek�nk egy Attributum adatstrukt�ra
		 * l�nyeg�ben �gy n�z ki hogy van 5 adattagja: 
		 * 1.name : maga a t�bl�zatbeli oszlop/attributum neve
		 * 2.type : az attributum tipusa (int,float...)
		 * 3.constraint : PK, FK vagy semmi
		 * 4.refTable - ha 3 FK akkor ebben a t�bl�ban van az elem amire mutat maga az FK
		 * 5.refAttr - ha 3 FK akkor ez az oszlop az el�bb kiv�lasztott t�bl�ban amire mutat az FK
		 * 
		 * van erre k�t konstruktor:
		 * - (nev,tipus,constraint)
		 * - (nev,tipus)
		 * beallitja nevet �s a tipust, els� eset�ben az adott constrintet is, m�sodik esetben a constarint automatikusan NONE lesz
		 * setConstraint metodust nem irtam, hogy a mez� constraint tipus�t csak az elej�n lehessen be�llitani, ha gondolod hogy ez nem praktikus
		 * akkor sz�lj �s irunk egyet, ezent�l van a setReference metodus amivel be tudjuk �llitani hogy az adott attributum melyik t�bl�ra, �s annak melyik
		 * elem�re mutat, ezt persze csak akkor engedi ha a konstruktorban az attributum FK-nak volt be�llitva
		 * 
		 * Nam�rmost, a formmal az egy bajom az, hogy nem �rtettem az FK mez�nek mi�rt textboxot adt�l �s nem radiobuttont mint a PKn�l
		 * pl, alapelk�pzel�s szerint �gye a Foreign Keynek nincs saj�t neve amit a felhaszn�l� ad meg, igy nem tudom oda mit lehetne beirni
		 * az Alter table add constraintn�l volt anno olyan lehet�s�g hogy mag�nak a constraintnek ad a felhaszn�l� nevet, de ezzel mi nem kell
		 * foglalkozzunk, mert n�lunk az hogy "Constraint" ink�bb szimbolikus, egy v�ltoz� n�v mint egy konkr�t dolog
		 * 
		 * Ez�rt �n azt az esetet aj�nlom, hogy �gyan�gy mint a PK az FK is egy radioButton, mert v�g�lis csak ki kell v�lasztani hogy aze, vagy sem
		 * �gye mivel a projektben nincsenek �sszetett kulcsok, egy attributum nem lehet egyszerre PK �s FK is, ez�rt ha m�r mindkett� radioButton
		 * akkor annyit k�ne m�g megoldani, hogy egyszerre ne lehessen csak az egyiket kiv�lasztani, ez EASY elm�letileg, mert l�trehozol egy
		 * olyat hogy ButtonGroup vagy ToggleGroup most tudja a hal�l melyik, abba beteszed a k�t gombot �s ez a probl�ma automatikusan el van int�zve,
		 * azt�n ez annyi probl�m�val j�rhat, hogy akkor nem tudjuk lek�rni egyenesen az adott gombokt�l hogy be vannake jel�lve, de ha ez lenne az eset amit nem hiszek
		 * akkor �gyis van mag�nak a groupnak olyan metodusa hogy getSelected vagy hasonl�, akkor azt lek�rj�k �s annak f�ggv�ny�ben �tirjuk ezt a f�ggv�nyt
		 * 
		 * miut�n hozz�adtuk a list�hoz t�r�lj�k a cuccokat a mez�b�l... azt �gy l�tom �gyis megirtad a metodust
		 * 
		 * Nam�rmost, ezek csak esetleges magyar�zatok, tan�csok, bla bla bla, semmi sem k�t�tt
		 * 
		 * Att�l f�ggetlen�l hogy mennyi az id�, ha valamivel megakadsz, nem �rted, k�ne egy megbesz�l�s, refresh a gondolatoknak, tan�cs, (ezt most tov�bb nem sorolom :D),
		 * irj r�m, val�szin� am�gyis vagy sorozatot fogok n�zni, vagy Csabival dum�lunk olyan hajnali 1-2-3-ig :))), elv�gre egy csapat vagyunk :D
		 * Addig is pihend ki kicsit magad �s ne agg�dj mert meglesz�nk :D, ha ez meg van akkor a Drop Table, Create Index m�r easy, ott a sz�p
		 * megold�s hogy mindennek comboboxa van �s att�l f�gg�en, hogy az el�z�ben mit v�lasztott�l ki friss�l a k�vetkez� tartalma + create indexn�l egy textbox a nev�nek
		 * (elm�letben nem t�nik v�szesnek, azt�n gyakorlatban te tudod, jobban), de ha most nincs id� ilyet lekezelni, vagy tudom �n mi, eml�kezz
		 * Server oldalon a DDL-ben hib�k szempontj�b�l minden is le van kezelve(ami eszembe jutott legal�bbis, de van el�g), sz�val ha van olyan lehet�s�g
		 * hogy ezekb�l a kliens valami faszs�got v�laszt ki, a server �gyis hiba�zenetet k�ld vissza neki :D
		 * */
		Attribute newAttr;
		String type = "";
		switch(attType.getSelectionModel().getSelectedItem()) {
		
		case "int":
			type = "int";
			break;
		case "datetime":
			type = "date";
			break;
		case "varchar (50)":
			type = "varchar50";
			break;
		case "float":
			type = "float";
			break;
		case "bit":
			type = "bit";
			break;
		}
		if(attPK.isSelected()) {
			
			newAttr = new Attribute(attName.getText(),type,Constraints.PRIMARY_KEY);
		} else {
			
			/*
			 * if given attribute is selected to be a foreign key than : {
			 *  newAttr = new Attribute(attName.getText(),type,Constraints.FOREIGN_KEY);
			 *  newAttr.setReference(attFKTable.getSelectionModel().getSelectedItem(),attFKAtt.getSelectionModel().getSelectedItem());
			 *  } else {
			 *  newAttr = new Attribute(attName.getText(),type);
			 *  }
			 * */
		}
		/*
		list.add(newAttr);
		clearAll();*/ // ha jol ertettem ugy mondtad, hogy akkor az uj attr-rt hozzaadjuk a listahoz, utana ertelem szeruen toroljuk a regi adatokat
	}
	
	@FXML
	public void clearAll() {
		attFKTable.getSelectionModel().clearSelection();
		attFKAtt.getSelectionModel().clearSelection();
		attType.getSelectionModel().clearSelection();
		attName.clear();
		tableName.clear();
		attFKName.clear();
	}
	
	@FXML
	public void setFKTable() {
		attFKTable.getItems().clear();
		attFKTable.getItems().addAll(DBStructure.getTables(databaseName.getSelectionModel().getSelectedItem()));
	}
	
	@FXML
	public void setFKAtt() {
		attFKAtt.getItems().clear();
		attFKAtt.getItems().addAll(DBStructure.getAttributes(databaseName.getSelectionModel().getSelectedItem(), attFKTable.getSelectionModel().getSelectedItem()));
	}
	
	@FXML
	public void createTable() throws IOException {
		Message msg = new Message();
		msg.setMsType(MessageType.CREATE_TABLE);
		msg.setDBname(databaseName.getSelectionModel().getSelectedItem());
		msg.setTbname(tableName.getText());
		msg.setColumns(list);
		Listener.sendRequest(msg);
		stage.close();
	}
	
	@FXML
	public void cancelPopup() {
		stage.close();
	}
	
	public void setStage(Stage stage) {
		this.stage = stage;
	}
	
}
