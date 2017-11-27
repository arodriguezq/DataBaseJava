import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

public class Main {

    public static void main(String[] args) throws ClassNotFoundException, ParserConfigurationException, SAXException, IOException {
        String url = "jdbc:postgresql://localhost:5432/training";
        String user = "iam47662285";
        String password = "MY56KZDZ";

        long tiempo = System.currentTimeMillis();
        ConectionManager conectionManager = new ConectionManager();
        conectionManager.connect(user,password,url);
       conectionManager.dropTables();
        conectionManager.createTables();
        conectionManager.fill();
        conectionManager.muestraCiudad("Spain");
        System.out.println((System.currentTimeMillis()-tiempo)/1000.);

    }
}
