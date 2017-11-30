package derbyDataBase;


import Models.City;
import Models.Country;
import xPath.MyReader;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class DerbyQuerys {
    private Connection connection;
    private MyReader reader;
    private String DERBYDB = "jdbc:derby:cityDB;create=true";
    static final int BATCHSIZE = 10;
    static int count = 0;

    public void connect() {
        try {
            connection = DriverManager.getConnection(DERBYDB);
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public void deleteTable() throws SQLException {

        try {
            connection.createStatement().executeUpdate("DROP TABLE MY_TABLE");
        } catch (SQLException e) {
            if (!e.getSQLState().equals("proper SQL-state for table does not exist"))
                throw e;
        }
    }

    public void createTable() throws SQLException {

        String query;
        query = "CREATE TABLE country (" +
                "country_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY," +
                "name varchar(40) NOT NULL )";
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.executeUpdate();

        System.out.println("Creation of table country -> OK");

        query = "CREATE TABLE city (" +
                "city_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY," +
                "country_id integer," +
                "name varchar(40) NOT NULL," +
                "CONSTRAINT fk_city_key FOREIGN KEY (country_id) REFERENCES country(country_id))";
        preparedStatement = connection.prepareStatement(query);
        preparedStatement.executeUpdate();
        System.out.println("Creation of table city -> OK");
        preparedStatement.close();
    }

    public void fill() throws IOException, SAXException, ParserConfigurationException, SQLException {

        reader = new MyReader("world.xml");
        List<String> country = reader.listCountry();

        fillCountry(country);


    }

    private void fillCountry(List<String> country) throws SQLException {
        String query = "INSERT INTO country (name) VALUES (?)";
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

        fillCity(country);
    }


    private void fillCity(List<String> country) {
        String query = "INSERT INTO city (name,country_id) VALUES " +
                "(?,(SELECT country_id from country where name = ?))";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            for (String countryName : country) {
                List<String> city = reader.listCity(countryName);

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

    public void closeDB() {

        try {
            DriverManager.getConnection("jdbc:derby:;shutdown=true");
        } catch (SQLException se) {
            // SQL State XJO15 and SQLCode 50000 mean an OK shutdown.
            if (!(se.getErrorCode() == 50000) && (se.getSQLState().equals("XJ015")))
                System.err.println(se);
        }
    }

    public void muestraCiudad(String countryName) {


        String query = "SELECT * FROM city where country_id = (select country_id from country where name =?)";
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(String.format("%-10s%-10s%-30s\n", "City_id 1 |", "Country_id |", "Name"));
        stringBuilder.append("---------+------------+-------------------------------------\n");

        try {

            PreparedStatement preparedStatement = connection.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            preparedStatement.setString(1, countryName);
            ResultSet resultado = preparedStatement.executeQuery();

            if (!resultado.isBeforeFirst()) {

            }

            while (resultado.next()) {

                int city_id = resultado.getInt("city_id");
                int country_id = resultado.getInt("country_id");
                String name = resultado.getString("name");
                stringBuilder.append(String.format("%10s%-12s%-30s\n", city_id + " |", country_id, "| " + name));
            }

            resultado.close();

        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println(stringBuilder.toString());
    }

    public void showAllCitys() {

        String query = "SELECT city_id,country_id, name FROM city";
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(String.format("%10s%-25s%-30s\n", "City_id |", "Country_id |", "Name"));
        stringBuilder.append("---------+-------------------------+-------------------------------------\n");

        try {

            PreparedStatement preparedStatement = connection.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet resultado = preparedStatement.executeQuery();


            while (resultado.next()) {

                int city_id = resultado.getInt(1);
                int country_id = resultado.getInt(2);
                String name = resultado.getString(3);
                stringBuilder.append(String.format("%10s%-25s%-30s\n", city_id + " |", country_id, "| " + name));
            }

            resultado.close();

        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println(stringBuilder.toString());

    }

    public void showAllCountries() {

        String query = "SELECT * FROM country order by 1";
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(String.format("%10s%-25s\n", "Country_id |", "name"));
        stringBuilder.append("---------+-------------------------\n");

        try {

            PreparedStatement preparedStatement = connection.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet resultado = preparedStatement.executeQuery();


            while (resultado.next()) {


                int country_id = resultado.getInt(1);
                String name = resultado.getString(2);
                stringBuilder.append(String.format("%10s%-25s\n", country_id + " |", name));
            }

            resultado.close();

        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println(stringBuilder.toString());

    }

    public void listCountries() {

        List<Country> countryList = new ArrayList<>();
        String query = "SELECT * FROM country";
        try {

            PreparedStatement preparedStatement = connection.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet resultado = preparedStatement.executeQuery();




            while (resultado.next()) {


                int country_id = resultado.getInt(1);
                String name = resultado.getString(2);
                Country country = new Country(country_id, name);


                String query2 = "SELECT * FROM city where country_id = (select country_id from country where name = ? order by 1)";

                try {

                    PreparedStatement preparedStatemen2 = connection.prepareStatement(query2, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                    preparedStatemen2.setString(1, country.getName());
                    ResultSet resultado2 = preparedStatemen2.executeQuery();



                    while (resultado2.next()) {

                        int city_id = resultado2.getInt(1);
                        String cityName = resultado2.getString(3);
                        City city = new City(city_id,cityName,country);
                        country.addCity(city);
                    }

                    resultado2.close();

                } catch (SQLException e) {
                    System.err.println("Error: " + e.getMessage());
                }

                countryList.add(country);
            }

            resultado.close();

        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
        }


        System.out.println(countryList.toString());


    }


}
