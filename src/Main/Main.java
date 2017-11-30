package Main;

import postgresDataBase.ConectionManager;
import derbyDataBase.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Main {

    public static void main(String[] args) throws ClassNotFoundException, ParserConfigurationException, SAXException, IOException, SQLException {


        String url = "jdbc:postgresql://localhost:5432/training";
        String user = "iam47662285";
        String password = "MY56KZDZ";

        ConectionManager conectionManager = new ConectionManager();
        conectionManager.connect(user,password,url);
        conectionManager.dropTables();
        conectionManager.createTables();
        conectionManager.fill();



        DerbyQuerys derbyQuerys = new DerbyQuerys();
        derbyQuerys.connect();
        derbyQuerys.createTable();
        derbyQuerys.fill();
        derbyQuerys.listCountries();




        derbyQuerys.closeDB();

        try {
            DriverManager.getConnection("jdbc:derby:;shutdown=true");
        }
        catch (SQLException se) {
            // SQL State XJO15 and SQLCode 50000 mean an OK shutdown.
            if (!(se.getErrorCode() == 50000) && (se.getSQLState().equals("XJ015")))
                System.err.println(se);
        }





    }
}
