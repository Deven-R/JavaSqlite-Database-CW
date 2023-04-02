import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.Scanner;
import java.util.*;

public class Database{

    private Connection connect()
	{
		Connection conn = null;
        try {
        String url = "jdbc:sqlite:TRY.db";
            // create a connection to the database
            conn = DriverManager.getConnection(url);
            System.out.println("Connection to SQLite has been established.");
        } 
        catch (SQLException e) 
        {
            System.out.println(e.getMessage());
        } 
        return conn;
	}

    //java -classpath ".:sqlite-jdbc-3.20.1.jar" Database.java 

    public void CreateAllTable() 
	 {
		
		 Connection conn = null;
		try {
	        	System.out.println("----------------------------------------");
	        	System.out.println("Creating All Tables");
                System.out.println("----------------------------------------");
	        	conn = this.connect();
	        	Statement playerStatement = conn.createStatement();
	        	int r1 = playerStatement.executeUpdate("CREATE TABLE IF NOT EXISTS Player" +
	        	"(AccountNum INTEGER NOT NULL, Surname VARCHAR (20), Forename VARCHAR (20), Name VARCHAR(20), PRIMARY KEY (AccountNum),FOREIGN KEY (Name) REFERENCES HasCharacter(Name) ON DELETE SET NULL)");
	        	 System.out.println("Player Table is created");

                 Statement HasCharStatement = conn.createStatement();
	        	int r2 = HasCharStatement.executeUpdate("CREATE TABLE IF NOT EXISTS HasCharacter" +
	        	"(Created_Date TEXT,Expiry_Date TEXT,Name VARCHAR(20) NOT NULL UNIQUE,Character_Type VARCHAR(20),Health INTEGER,AccountNum INTEGER NOT NULL,PRIMARY KEY (AccountNum,Name),FOREIGN KEY (AccountNum) REFERENCES Player(AccountNum) ON DELETE CASCADE)");
	        	 System.out.println("HasCharacterTable is created");

                 Statement BattleEnemy = conn.createStatement();
	        	int r3 = BattleEnemy.executeUpdate("CREATE TABLE IF NOT EXISTS BattleWithEnemy" +
	        	"(Attacker VARCHAR(20),Weapon VARCHAR(20), AccountNum INTEGER UNIQUE,Name VARCHAR(20) UNIQUE, PRIMARY KEY (Attacker),FOREIGN KEY (AccountNum, Name) REFERENCES HasCharacter(AccountNum,Name) ON DELETE SET NULL)");
	        	 System.out.println("Battle Enemy is created");

                 Statement Billed = conn.createStatement();
	        	int r4 = Billed.executeUpdate("CREATE TABLE IF NOT EXISTS BilledPayment" +
	        	"(Email TEXT NOT NULL, Money_Bank REAL, AccountNum INTEGER NOT NULL, PRIMARY KEY (AccountNum, Email), FOREIGN KEY (AccountNum) REFERENCES Player (AccountNum) ON DELETE CASCADE)");
	        	 System.out.println("Billed Payment is created");

                 Statement CombatInfo = conn.createStatement();
	        	int r5 = CombatInfo.executeUpdate("CREATE TABLE IF NOT EXISTS HasCombatInfo" +
	        	"(Battle_No INTEGER NOT NULL, Battle_Date TEXT, Result VARCHAR(20), Name VARCHAR(20) NOT NULL UNIQUE, AccountNum INTEGER NOT NULL UNIQUE, PRIMARY KEY(Name, Battle_No, AccountNum), FOREIGN KEY(Name,AccountNum) REFERENCES HasCharacter(Name,AccountNum) ON DELETE CASCADE)");
	        	 System.out.println("HasCombatInfo is created");

                 Statement PossInv = conn.createStatement();
	        	int r6 = PossInv.executeUpdate("CREATE TABLE IF NOT EXISTS PossessInventory" +
	        	"(Character VARCHAR(20) NOT NULL, Name VARCHAR(20), AccountNum INTEGER, PRIMARY KEY(Character), FOREIGN KEY (Name,AccountNum) REFERENCES HasCharacter (Name,AccountNum) ON DELETE SET NULL)"); //Needs to be checked
	        	 System.out.println("PossessInventory is created");

                 Statement Invweapon = conn.createStatement();
                 int r7 = Invweapon.executeUpdate("CREATE TABLE IF NOT EXISTS Inventory_Weapon" +
                 "(Character VARCHAR(20) NOT NULL, Item VARCHAR(20) NOT NULL,PRIMARY KEY (Character), FOREIGN KEY (Item) REFERENCES Weapons(Item) ON DELETE CASCADE)"); //Needs to be checked
                  System.out.println("InvWeapons is created");

                  Statement Invsupplies = conn.createStatement();
                 int r8 = Invsupplies.executeUpdate("CREATE TABLE IF NOT EXISTS Inventory_Supplies" +
                 "(Character VARCHAR(20) NOT NULL, Item VARCHAR(20) NOT NULL,PRIMARY KEY (Character), FOREIGN KEY (Item) REFERENCES Supplies(Item) ON DELETE CASCADE)"); //Needs to be checked
                  System.out.println("InvSupplies is created");

                  Statement Invarmor = conn.createStatement();
                 int r9 = Invarmor.executeUpdate("CREATE TABLE IF NOT EXISTS Inventory_Armor" +
                 "(Character VARCHAR(20) NOT NULL, Item VARCHAR(20) NOT NULL,PRIMARY KEY (Character), FOREIGN KEY (Item) REFERENCES Armor(Item) ON DELETE CASCADE)"); 
                  System.out.println("InvArmor is created");

                  Statement Weapons = conn.createStatement();
                 int r10 = Weapons.executeUpdate("CREATE TABLE IF NOT EXISTS Weapons" +
                 "(Item VARCHAR(20) NOT NULL, Range INTEGER,Attack_Score INTEGER, Weapon_Type VARCHAR(20), PRIMARY KEY (Item))"); 
                  System.out.println("Weapons is created");

                  Statement Supply = conn.createStatement();
                 int r11 = Supply.executeUpdate("CREATE TABLE IF NOT EXISTS Supplies" +
                 "(Item VARCHAR(20) NOT NULL, Mana_Score INTEGER, Quantity INTEGER, Healing_Score INTEGER, Item_Type VARCHAR(20),PRIMARY KEY (Item))"); 
                  System.out.println("Supply is created");

                  Statement Armor = conn.createStatement();
                 int r12 = Armor.executeUpdate("CREATE TABLE IF NOT EXISTS Armor" +
                 "(Item VARCHAR(20) NOT NULL, Body_Part VARCHAR(20), Quantity INTEGER, Defence_Score INTEGER,PRIMARY KEY (Item))"); 
                  System.out.println("Armor is created");

                }
	        catch (SQLException e) {
	            System.out.println(e.getMessage());
	        }finally { try {
	        	
	            if (conn != null) {
	                conn.close();
	            }
	        } catch (SQLException ex) {
	            System.out.println(ex.getMessage());
	        	}}
	        
	    }

        public void import_insert(String FileCust, String FileComb, String FileItem){

            System.out.println("----------------------------------------");
            Connection conn = this.connect();
  

            String line = "";
            ArrayList<String> Account_Num = new ArrayList<String>();
            ArrayList<String> Forename = new ArrayList<String>();
            ArrayList<String> Surname = new ArrayList<String>();
            ArrayList<String> Email = new ArrayList<String>(); 
            ArrayList<String> Creation_Date = new ArrayList<String>();
            ArrayList<String> Expiry_Date = new ArrayList<String>();
            ArrayList<String> CharName = new ArrayList<String>();
            ArrayList<String> CharType = new ArrayList<String>();
            ArrayList<String> Health = new ArrayList<String>();
            ArrayList<String> Money_Bank = new ArrayList<String>();

            //------------------------------------------------
            ArrayList<String> BattleDate = new ArrayList<String>();
            ArrayList<String> BattleNo = new ArrayList<String>(); 
            ArrayList<String> Attacker = new ArrayList<String>();
            ArrayList<String> Weapon = new ArrayList<String>();
            ArrayList<String> Result = new ArrayList<String>(); 
            ArrayList<String> Character = new ArrayList<String>();
            ArrayList<String> Item = new ArrayList<String>();
            ArrayList<String> ItemType = new ArrayList<String>();
            ArrayList<String> WeaponType = new ArrayList<String>();
            ArrayList<String> Range = new ArrayList<String>();
            ArrayList<String> Quantity = new ArrayList<String>();
            ArrayList<String> DefendScore = new ArrayList<String>();
            ArrayList<String> AttackScore = new ArrayList<String>();
            ArrayList<String> HealingScore = new ArrayList<String>();
            ArrayList<String> ManaScore = new ArrayList<String>(); 
            ArrayList<String> BodyPart = new ArrayList<String>(); 

            System.out.println("----------------------------------------");
            System.out.println("Populating Tables");
            System.out.println("----------------------------------------");

            try {
                BufferedReader buffCust = new BufferedReader(new FileReader(FileCust));
                buffCust.readLine();
                while ((line = buffCust.readLine()) != null) {
                    String [] custvalues = line.split(",");
                    
                    Account_Num.add(custvalues[0]);
                    Forename.add(custvalues[1]);
                    Surname.add(custvalues[2]);
                    Email.add(custvalues[3]);
                    Creation_Date.add(custvalues[4]);
                    Expiry_Date.add(custvalues[5]);
                    CharName.add(custvalues[6]);
                    CharType.add(custvalues[7]);
                    Health.add(custvalues[11]);
                    Money_Bank.add(custvalues[16]);

                  //  System.out.println(Account_Num);

                }
            } catch (Exception e) {
                // TODO: handle exception
            }

            try {
                BufferedReader buffComb = new BufferedReader(new FileReader(FileComb));
                buffComb.readLine();
                while ((line = buffComb.readLine()) != null) {
                    String [] combvalues = line.split(",");

                    BattleDate.add(combvalues[0]);
                    BattleNo.add(combvalues[1]);
                    Attacker.add(combvalues[2]);
                    Weapon.add(combvalues[4]);
                    Result.add(combvalues[5]);

                //    System.out.println(BattleDate);

                    }
            } catch (Exception e) {
                // TODO: handle exception
            }

            try {
                BufferedReader buffitem = new BufferedReader(new FileReader(FileItem));
                buffitem.readLine();
                while ((line = buffitem.readLine()) != null) {
                    String [] itemvalues = line.split(",");

                    Character.add(itemvalues[0]);
                    Item.add(itemvalues[1]);
                    ItemType.add(itemvalues[2]);
                    WeaponType.add(itemvalues[3]);
                    Range.add(itemvalues[4]);
                    Quantity.add(itemvalues[6]);
                    DefendScore.add(itemvalues[7]);
                    AttackScore.add(itemvalues[8]);
                    HealingScore.add(itemvalues[9]);
                    ManaScore.add(itemvalues[10]);
                    BodyPart.add(itemvalues[14]);

                  

              

      }
            } catch (Exception e) {
                // TODO: handle exception
            }

            
            String insertplayer = "INSERT INTO Player (AccountNum, Surname, Forename, Name) VALUES (?,?,?,?)";
          
            for (int i = 0; i < Account_Num.size(); i++) {
                try { 
                    
                    PreparedStatement pstmt = conn.prepareStatement(insertplayer);
                    
                        pstmt.setInt(1, Integer.parseInt(Account_Num.get(i)));
                        pstmt.setString(2, Surname.get(i));
                        pstmt.setString(3, Forename.get(i));
                        pstmt.setString(4, CharName.get(i));

                        pstmt.execute(); 

                        System.out.println("Player data inserted");

                    } catch (SQLException e) {
                    System.out.println(e.getMessage());
                }
            }

            System.out.println("----------------------------------------");

            System.out.println("Player data inserted");

            System.out.println("----------------------------------------");

         String inserthaschar = "INSERT INTO HasCharacter (Created_Date, Expiry_Date, Name, Character_Type, Health, AccountNum) VALUES (?,?,?,?,?,?)";
         
         for (int i = 0; i < Account_Num.size(); i++) {
            try {
                PreparedStatement charpstmt  = conn.prepareStatement(inserthaschar);
                    charpstmt.setString(1, Creation_Date.get(i));
                    charpstmt.setString(2, Expiry_Date.get(i));
                    charpstmt.setString(3, CharName.get(i));
                    charpstmt.setString(4, CharType.get(i));
                    charpstmt.setString(5, Health.get(i));
                    charpstmt.setInt(6, Integer.parseInt(Account_Num.get(i)));

                    charpstmt.execute();

                    


                } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            
        }
        System.out.println("----------------------------------------");
        System.out.println("HasCharacter data inserted");
        System.out.println("----------------------------------------");

        String insertbattlee = "INSERT INTO BattleWithEnemy (Attacker, Weapon, AccountNum, Name) VALUES (?,?,?,?)";
         
         for (int i = 0; i < Attacker.size(); i++) {
            try {
                PreparedStatement battpstmt  = conn.prepareStatement(insertbattlee);
                    battpstmt.setString(1, Attacker.get(i));
                    battpstmt.setString(2, Weapon.get(i));
                    battpstmt.setInt(3, Integer.parseInt(Account_Num.get(i)));
                    battpstmt.setString(4, CharName.get(i));
                    

                    battpstmt.execute();

                    


                } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            
        }
        System.out.println("----------------------------------------");
        System.out.println("BattleWithEnemy data inserted");
        System.out.println("----------------------------------------");

        String insertbill = "INSERT INTO BilledPayment (Email,Money_Bank,AccountNum) VALUES (?,?,?)";
         
         for (int i = 0; i < Email.size(); i++) {
            try {
                PreparedStatement billpstmt  = conn.prepareStatement(insertbill);
                    billpstmt.setString(1, Email.get(i));
                    billpstmt.setDouble(2, Double.parseDouble(Money_Bank.get(i)));
                    billpstmt.setInt(3, Integer.parseInt(Account_Num.get(i)));
                    billpstmt.execute();

                    
          } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            
        }
        System.out.println("----------------------------------------");
        System.out.println("BilledPayment data inserted");
        System.out.println("----------------------------------------");


        String insertcombat = "INSERT INTO HasCombatInfo (Battle_No,Battle_Date,Result, Name, AccountNum) VALUES (?,?,?,?,?)";
         
         for (int i = 0; i < BattleNo.size(); i++) {
            try {
                PreparedStatement pstmt  = conn.prepareStatement(insertcombat);
                    pstmt.setInt(1, Integer.parseInt(BattleNo.get(i)));
                    pstmt.setString(2, BattleDate.get(i));
                    pstmt.setString(3, Result.get(i));
                    pstmt.setString(4, CharName.get(i));
                    pstmt.setInt(5, Integer.parseInt(Account_Num.get(i)));
                    pstmt.execute();

                    
          } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            
        }
        System.out.println("----------------------------------------");
        System.out.println("HasCombatInfo data inserted");
        System.out.println("----------------------------------------");

        String insertpossinv = "INSERT INTO PossessInventory (Character, Name, AccountNum) VALUES (?,?,?)";
         
         for (int i = 0; i < Account_Num.size(); i++) {
            try {
                PreparedStatement pstmt  = conn.prepareStatement(insertpossinv);
                    pstmt.setString(1, Character.get(i));
                    pstmt.setString(2, CharName.get(i));
                    pstmt.setInt(3, Integer.parseInt(Account_Num.get(i)));

                    
                    pstmt.execute();

                    
          } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            
        }
        System.out.println("----------------------------------------");
        System.out.println("PossessInventory data inserted");
        System.out.println("----------------------------------------");

        String insertinvweapon = "INSERT INTO Inventory_Weapon (Character,Item) VALUES (?,?)";
         
         for (int i = 0; i < Character.size(); i++) {
            try {

                if(ItemType.get(i).equals("Weapon")){

                    PreparedStatement pstmt  = conn.prepareStatement(insertinvweapon);

                    pstmt.setString(1, Character.get(i));
                    pstmt.setString(2, Item.get(i));
                    
                    pstmt.execute();

                }
                
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            
        }
        System.out.println("----------------------------------------");
        System.out.println("Inventory_Weapon data inserted");
        System.out.println("----------------------------------------");

        String insertinvsupply = "INSERT INTO Inventory_Supplies (Character,Item) VALUES (?,?)";
         
         for (int i = 0; i < Character.size(); i++) {
            try { 

                if(!ItemType.get(i).equals("Weapon") && !ItemType.get(i).equals("Armour")){
                
                PreparedStatement pstmt  = conn.prepareStatement(insertinvsupply);
                    pstmt.setString(1, Character.get(i));
                    pstmt.setString(2, Item.get(i));
                    
                    pstmt.execute();
                }

                    
          } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            
        }
        System.out.println("----------------------------------------");
        System.out.println("Inventory_Supplies data inserted");
        System.out.println("----------------------------------------");

        String insertinvarm = "INSERT INTO Inventory_Armor (Character,Item) VALUES (?,?)";
         
         for (int i = 0; i < Character.size(); i++) {
            try { 

                if(ItemType.get(i).equals("Armour")){
                
                PreparedStatement pstmt  = conn.prepareStatement(insertinvarm);
                    pstmt.setString(1, Character.get(i));
                    pstmt.setString(2, Item.get(i));
                    
                    pstmt.execute();
                }

                    
          } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            
        }
        System.out.println("----------------------------------------");
        System.out.println("Inventory_Armor data inserted");
        System.out.println("----------------------------------------");

        String insertweapon = "INSERT INTO Weapons (Item, Range, Attack_Score, Weapon_Type) VALUES (?,?,?,?)";
         
        for (int i = 0; i < Item.size(); i++) {
           try { 

               if(ItemType.get(i).equals("Weapon")){
               
               PreparedStatement pstmt  = conn.prepareStatement(insertweapon);
                   pstmt.setString(1, Item.get(i));
                   
                   if (Range.get(i).length() == 0) {
                    pstmt.setNull(2, java.sql.Types.NULL);
                   } else {
                       pstmt.setInt(2, Integer.parseInt(Range.get(i)));
                    }
                    
                    if(AttackScore.get(i).length() == 0){
                        pstmt.setNull(3, java.sql.Types.NULL);
                    } else {
                        pstmt.setInt(3, Integer.parseInt(AttackScore.get(i)));
                    }

                   pstmt.setString(4,WeaponType.get(i));                   
                   pstmt.execute();
               }

                   
         } catch (SQLException e) {
               System.out.println(e.getMessage());
           }
           
       }
       System.out.println("----------------------------------------");
       System.out.println("Weapon data inserted");
       System.out.println("----------------------------------------");

       String insertsupply = "INSERT INTO Supplies (Item, Mana_Score, Quantity, Healing_Score, Item_Type) VALUES (?,?,?,?,?)";
         
       for (int i = 0; i < Item.size(); i++) {
          try { 

              if(!ItemType.get(i).equals("Weapon") && !ItemType.get(i).equals("Armour")){
              
              PreparedStatement pstmt  = conn.prepareStatement(insertsupply);
                  pstmt.setString(1, Item.get(i));

                  if (ManaScore.get(i).length() == 0) {
                    pstmt.setNull(2, java.sql.Types.NULL);
                   } else {
                       pstmt.setInt(2, Integer.parseInt(ManaScore.get(i)));
                    }

                    if (HealingScore.get(i).length() == 0) {
                        pstmt.setNull(4, java.sql.Types.NULL);
                       } else {
                           pstmt.setInt(4, Integer.parseInt(HealingScore.get(i)));
                        }

                  pstmt.setInt(3, Integer.parseInt(Quantity.get(i)));

                  pstmt.setString(5,ItemType.get(i));                   
                  pstmt.execute();
              }

                  
        } catch (SQLException e) {
              System.out.println(e.getMessage());
          }
          
      }
      System.out.println("----------------------------------------");
      System.out.println("Supply data inserted");
      System.out.println("----------------------------------------");

      String insertarmor = "INSERT INTO Armor (Item, Body_Part, Quantity, Defence_Score) VALUES (?,?,?,?)";
         
      for (int i = 0; i < Item.size(); i++) {
         try { 

             if(ItemType.get(i).equals("Armour")){
             
             PreparedStatement pstmt  = conn.prepareStatement(insertarmor);
                 pstmt.setString(1, Item.get(i));
                 pstmt.setString(2, BodyPart.get(i));
                 pstmt.setInt(3, Integer.parseInt(Quantity.get(i)));
                 pstmt.setInt(4, Integer.parseInt(DefendScore.get(i)));


                                    
                 pstmt.execute();
             }

                 
       } catch (SQLException e) {
             System.out.println(e.getMessage());
         }
         
     }
     System.out.println("----------------------------------------");
     System.out.println("Armor data inserted");
     System.out.println("----------------------------------------");


    }

public void select_queries(){
    Connection conn = this.connect();
    System.out.println("----------------------------------------");
    System.out.println("List the top 5 characters with the highest number of successful combats attacks:");

        String q1 = "SELECT Name, COUNT(*) as SuccessAttacks FROM HasCombatInfo WHERE (Result = 'Hit') OR (Result = 'Victory') GROUP BY Name ORDER BY SuccessAttacks DESC LIMIT 5";
        
        try (Statement query  = conn.createStatement();
             ResultSet qr1    = query.executeQuery(q1)){
            
            // loop through the result set
            while (qr1.next()) {
                System.out.println(qr1.getString("Name") +  "\t" + qr1.getInt("SuccessAttacks"));
               
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        
        System.out.println("----------------------------------------");
        System.out.println("Print the name and total number of attacks per character having more than 5 attacks: ");
        String q2 = "SELECT Name, COUNT(*) as TotalAttacks FROM HasCombatInfo WHERE (Result = 'Hit') OR (Result = 'Victory') OR (Result = 'Parry') OR (Result = 'Miss') GROUP BY Name HAVING COUNT(*) > 5";
        
        try (Statement query  = conn.createStatement();
             ResultSet qr2    = query.executeQuery(q2)){
            
            // loop through the result set
            while (qr2.next()) {
                System.out.println(qr2.getString("Name") +  "\t" + qr2.getInt("TotalAttacks"));
               
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        System.out.println("----------------------------------------");
        System.out.println("Order the names of characters from highest to lowest number of attacks. : ");
        String q3 = "SELECT Name, COUNT(*) as Attacks FROM HasCombatInfo GROUP BY Name ORDER BY Attacks DESC";
        
        try (Statement query  = conn.createStatement();
             ResultSet qr3    = query.executeQuery(q3)){
            
            // loop through the result set
            while (qr3.next()) {
                System.out.println(qr3.getString("Name") +  "\t" + qr3.getInt("Attacks"));
               
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        
    

        System.out.println("----------------------------------------");
        System.out.println("List the name of Players with at least 5 characters:");
        String q4 = "SELECT Player.Forename, Player.Surname, COUNT(*) as NumofCharacters FROM Player JOIN HasCharacter ON Player.AccountNum = HasCharacter.AccountNum GROUP BY Player.Forename HAVING COUNT(*) >= 5";
        
        try (Statement query  = conn.createStatement();
             ResultSet qr4   = query.executeQuery(q4)){
            
            // loop through the result set
            while (qr4.next()) {
                System.out.println(qr4.getString("Forename") +  "\t" + qr4.getString("Surname") +  "\t" + qr4.getInt("NumofCharacters"));
               
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        
        System.out.println("----------------------------------------");
    }
    
   



public static void main(String[] args) {
    Database db = new Database();

    db.connect();
    db.CreateAllTable();
    db.import_insert("Customers.csv", "Combat.csv", "Items.csv");
    db.select_queries();
    
}




}