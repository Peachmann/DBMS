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
		 * itt csak kiszopja az adatokat a formból és beteszi a listbe
		 * amit az elõbb már inicializáltam
		 * az OK elküldi az üzenetet
		 */
		/*
		 * Namármost, ez jó, de nem értem pontosan hogy gondoltad itt el teljesen a strukturat, egyelõre nem irok át
		 * semmit, csak ezt a részletet irtam és adok pár javaslatot. Kezdjük az alapokkal, nekünk egy Attributum adatstruktúra
		 * lényegében úgy néz ki hogy van 5 adattagja: 
		 * 1.name : maga a táblázatbeli oszlop/attributum neve
		 * 2.type : az attributum tipusa (int,float...)
		 * 3.constraint : PK, FK vagy semmi
		 * 4.refTable - ha 3 FK akkor ebben a táblában van az elem amire mutat maga az FK
		 * 5.refAttr - ha 3 FK akkor ez az oszlop az elõbb kiválasztott táblában amire mutat az FK
		 * 
		 * van erre két konstruktor:
		 * - (nev,tipus,constraint)
		 * - (nev,tipus)
		 * beallitja nevet és a tipust, elsõ esetében az adott constrintet is, második esetben a constarint automatikusan NONE lesz
		 * setConstraint metodust nem irtam, hogy a mezõ constraint tipusát csak az elején lehessen beállitani, ha gondolod hogy ez nem praktikus
		 * akkor szólj és irunk egyet, ezentúl van a setReference metodus amivel be tudjuk állitani hogy az adott attributum melyik táblára, és annak melyik
		 * elemére mutat, ezt persze csak akkor engedi ha a konstruktorban az attributum FK-nak volt beállitva
		 * 
		 * Namármost, a formmal az egy bajom az, hogy nem értettem az FK mezõnek miért textboxot adtál és nem radiobuttont mint a PKnál
		 * pl, alapelképzelés szerint úgye a Foreign Keynek nincs saját neve amit a felhasználó ad meg, igy nem tudom oda mit lehetne beirni
		 * az Alter table add constraintnél volt anno olyan lehetõség hogy magának a constraintnek ad a felhasználó nevet, de ezzel mi nem kell
		 * foglalkozzunk, mert nálunk az hogy "Constraint" inkább szimbolikus, egy változó név mint egy konkrét dolog
		 * 
		 * Ezért én azt az esetet ajánlom, hogy úgyanúgy mint a PK az FK is egy radioButton, mert végülis csak ki kell választani hogy aze, vagy sem
		 * Úgye mivel a projektben nincsenek összetett kulcsok, egy attributum nem lehet egyszerre PK és FK is, ezért ha már mindkettõ radioButton
		 * akkor annyit kéne még megoldani, hogy egyszerre ne lehessen csak az egyiket kiválasztani, ez EASY elméletileg, mert létrehozol egy
		 * olyat hogy ButtonGroup vagy ToggleGroup most tudja a halál melyik, abba beteszed a két gombot és ez a probléma automatikusan el van intézve,
		 * aztán ez annyi problémával járhat, hogy akkor nem tudjuk lekérni egyenesen az adott gomboktól hogy be vannake jelölve, de ha ez lenne az eset amit nem hiszek
		 * akkor úgyis van magának a groupnak olyan metodusa hogy getSelected vagy hasonló, akkor azt lekérjük és annak függvényében átirjuk ezt a függvényt
		 * 
		 * miután hozzáadtuk a listához töröljük a cuccokat a mezõbõl... azt úgy látom úgyis megirtad a metodust
		 * 
		 * Namármost, ezek csak esetleges magyarázatok, tanácsok, bla bla bla, semmi sem kötött
		 * 
		 * Attól függetlenül hogy mennyi az idõ, ha valamivel megakadsz, nem érted, kéne egy megbeszélés, refresh a gondolatoknak, tanács, (ezt most tovább nem sorolom :D),
		 * irj rám, valószinû amúgyis vagy sorozatot fogok nézni, vagy Csabival dumálunk olyan hajnali 1-2-3-ig :))), elvégre egy csapat vagyunk :D
		 * Addig is pihend ki kicsit magad és ne aggódj mert megleszünk :D, ha ez meg van akkor a Drop Table, Create Index már easy, ott a szép
		 * megoldás hogy mindennek comboboxa van és attól függõen, hogy az elõzõben mit választottál ki frissül a következõ tartalma + create indexnél egy textbox a nevének
		 * (elméletben nem tûnik vészesnek, aztán gyakorlatban te tudod, jobban), de ha most nincs idõ ilyet lekezelni, vagy tudom én mi, emlékezz
		 * Server oldalon a DDL-ben hibák szempontjából minden is le van kezelve(ami eszembe jutott legalábbis, de van elég), szóval ha van olyan lehetõség
		 * hogy ezekbõl a kliens valami faszságot választ ki, a server úgyis hibaüzenetet küld vissza neki :D
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
