package postgresDataBase;


import org.xml.sax.SAXException;
import xPath.MyReader;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.sql.*;

import java.util.List;

public class ConectionManager {

    private Connection connection;
    private MyReader r;
    private PreparedStatement preparedStatement;
    private PreparedStatement preparedStatement2;
    private String user;
    private String password;
    private String urlDataBase;
    final private String TABLECITY = "city";
    final private String TABLECOUNTRY = "country";
    static final int BATCHSIZE = 10;
    static int count = 0;


    public ConectionManager() throws ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        System.out.println("PostgreSQL JDBC Driver Registered!");
    }

    public void connect(String user, String password, String urlDataBase) {
        try {
            connection = DriverManager.getConnection(urlDataBase, user, password);
            this.user = user;
            this.password = password;
            this.urlDataBase = urlDataBase;
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public void dropTables() {
        String query = "DROP TABLE IF EXISTS city,country";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.executeUpdate();
            preparedStatement.close();

            System.out.println("Delete tables -> OK");
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    public void createTables() {
        PreparedStatement statement = null;
        String query = null;

        try {

            query = "CREATE TABLE  country (" +
                    "country_id SERIAL PRIMARY KEY,\n" +
                    "name VARCHAR(40) NOT NULL)";
            statement = connection.prepareStatement(query);
            statement.executeUpdate();

            System.out.println("Creation of table country -> OK");

            query = "CREATE TABLE  city (" +
                    "city_id SERIAL PRIMARY KEY,\n" +
                    "country_id integer,\n" +
                    "name VARCHAR(40) NOT NULL,\n" +
                    "CONSTRAINT fk_city_key FOREIGN KEY (country_id) REFERENCES country(country_id))";

            System.out.println("Creation of table city -> OK");
            statement = connection.prepareStatement(query);
            statement.executeUpdate();
            statement.close();

        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }


    public void fill() throws IOException, SAXException, ParserConfigurationException {

        r = new MyReader("world.xml");
        List<String> country = r.listCountry();

        fillCountry(country);


    }

    private void fillCountry(List<String> country) {
        String query = "INSERT INTO country (name) VALUES (?)";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            for (String countryName : country) {
                preparedStatement.setString(1, countryName);
                preparedStatement.addBatch();
                if (++count % BATCHSIZE == 0) {
                    preparedStatement.executeBatch();
                }
            }
            preparedStatement.executeBatch();
            preparedStatement.close();
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
        }

        fillCity(country);


    }

    private void fillCity(List<String> country) {
        String query = "INSERT INTO city (name,country_id) VALUES " +
                "(?,(SELECT country_id from country where name = ?))";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            for (String countryName : country) {
                List<String> city = r.listCity(countryName);

                for (String cityName : city) {
                    preparedStatement.setString(1, cityName);
                    preparedStatement.setString(2, countryName);
                    preparedStatement.addBatch();
                    if (++count % BATCHSIZE == 0) {
                        preparedStatement.executeBatch();
                    }
                }
                preparedStatement.executeBatch();

            }
            preparedStatement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void muestraCiudad(String countryName){
        String query = "SELECT * FROM city where country_id = (select country_id from country where name =?)";
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(String.format("%-10s%-10s%-30s\n","City_id 1 |","Country_id |","Name"));
        stringBuilder.append("---------+------------+-------------------------------------\n");

        try {

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1,countryName);
            preparedStatement.setString(1,countryName);
            ResultSet resultado = preparedStatement.executeQuery();

            if(!resultado.next()){

            }

            while (resultado.next()){

                int city_id = resultado.getInt("city_id");
                int country_id = resultado.getInt("country_id");
                String name = resultado.getString("name");
                stringBuilder.append(String.format("%10s%-12s%-30s\n",city_id+" |",country_id,"| "+name));
            }

            resultado.close();

        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println(stringBuilder.toString());
    }

    public void deleteCity(String cityName){
        String query = "DELETE FROM city where country_id = (select country_id from country where name =?)";
        try {

            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.executeUpdate();
            preparedStatement.close();

            System.out.println("Models.City :"+cityName+" deleted");
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    public void updateCity(String cityName, String newCityName){
        String query ="UPDATE city SET name = ? where name =?";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1,newCityName);
            preparedStatement.setString(2,cityName);
            preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }








}
