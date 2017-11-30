package Backup;

import derbyDataBase.DerbyQuerys;
import postgresDataBase.ConectionManager;

import java.sql.*;

public class Main {
    static final int BATCHSIZE = 60;
    static int count = 0;

    public static void main(String[] args) throws SQLException, ClassNotFoundException {


        String url = "jdbc:postgresql://localhost:5432/training";
        String user = "iam47662285";
        String password = "MY56KZDZ";

        ConectionManager conectionPostgres = new ConectionManager();
        conectionPostgres.connect(user, password, url);
        Connection connection1 = conectionPostgres.getConnection();


        DerbyQuerys derbyQuerys = new DerbyQuerys();
        derbyQuerys.connect();
        derbyQuerys.createTable();
        Connection connection2 = derbyQuerys.getConnection();




        try (Statement statement1 = connection1.createStatement();Statement statement2 = connection1.createStatement();PreparedStatement insertStatement=
                connection2.prepareStatement("insert into country (name) values(?)");PreparedStatement insertStatement2=
                connection2.prepareStatement("insert into city(country_id, name) values(?, ?)")){
            try (final ResultSet resultSet =
                         statement1.executeQuery("select * from country"); final ResultSet resultSet2 =statement2.executeQuery("select * from city")) {
                while (resultSet.next()) {
                    // Get the values from the table1 record
                    final String name = resultSet.getString("name");

                    // Insert a row with these values into table2
                    insertStatement.clearParameters();

                    insertStatement.setString(1, name);
                    insertStatement.addBatch();
                    if (++count % BATCHSIZE == 0) {
                        insertStatement.executeBatch();
                    }

                }
                insertStatement.executeBatch();

                while (resultSet2.next()){

                    final String name = resultSet2.getString("name");
                    final int country_id = resultSet2.getInt("country_id");

                    // Insert a row with these values into table2
                    insertStatement2.clearParameters();

                    insertStatement2.setInt(1, country_id);
                    insertStatement2.setString(2, name);
                    insertStatement2.addBatch();
                    if (++count % BATCHSIZE == 0) {
                        insertStatement2.executeBatch();
                    }

                }
                insertStatement2.executeBatch();
            }
        }

        derbyQuerys.showAllCountries();
        derbyQuerys.showAllCitys();
    }
}
