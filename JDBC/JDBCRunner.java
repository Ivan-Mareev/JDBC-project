package JDBC;
import java.sql.*;
import java.util.*;

public class JDBCRunner {
    private static final String PROTOCOL = "jdbc:postgresql://";
    private static final String DRIVER = "org.postgresql.Driver";
    private static final String URL_LOCALE_NAME = "localhost/";
    private static final String DATABASE_NAME = "myDataBaseProject";
    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";
    public static final String DATABASE_PASS = "postgres";


    public static void main(String[] args) {
        // проверка возможности подключения
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            //All tables
            getCoins(connection); System.out.println();
            getExchangers(connection); System.out.println();
            getCities(connection); System.out.println();

            //
            getCoinExchangersCities(connection); System.out.println();

            //param
            getCitiesByPopulation(connection, 41000, 1000000); System.out.println();
            getCoinExchangers(connection, "USDT"); System.out.println();
            getExchangersId(connection, 3); System.out.println();
            getCoinExchangersCities(connection, "Москва"); System.out.println();

            //correction
            addExchanger(connection, "Obmen", 4); System.out.println();
            removeExchanger(connection, "Obmen", 4); System.out.println();
            correctCitiesPopulation(connection, "Торжок", 41116); System.out.println();
        } catch (SQLException e) {
            if (e.getSQLState().startsWith("23")){
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }


    public static void checkDriver () {
        try {
            //Если класс найден, то JDBC драйвер загружен и готов к использованию
            Class.forName(DRIVER);

        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB () {
        try {
            //установка соединения с БД, используя предоставленные параметры.
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }

    // Получить всю таблицу Coins
    private static void getCoins(Connection connection) throws SQLException {
        String columnName0 = "id", columnName1 = "name", columnName2 = "exchangers_id";
        int param0 = -1, param2 = -1;
        String param1 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM coins;");

        while (rs.next()) {
            param2 = rs.getInt(columnName2);
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);
            System.out.println(param0 + " | " + param1 + " | " + param2);
        }
    }

    // Получить всю таблицу Exchangers
    private static void getExchangers(Connection connection) throws SQLException {
        String columnName0 = "id", columnName1 = "name", columnName2 = "city_id";
        int param0 = -1, param2 = -1;
        String param1 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM exchangers;");

        while (rs.next()) {
            param2 = rs.getInt(columnName2);
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);
            System.out.println(param0 + " | " + param1 + " | " + param2);
        }
    }

    // Получить всю таблицу Cities
    private static void getCities(Connection connection) throws SQLException {
        String columnName0 = "id", columnName1 = "name", columnName2 = "population";
        int param0 = -1;
        String param1 = null, param2 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM cities;");

        while (rs.next()) {
            param2 = rs.getString(columnName2);
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);
            System.out.println(param0 + " | " + param1 + " | " + param2);
        }
    }

    // Получить города, где население от value1 до value2
    private static void getCitiesByPopulation(Connection connection, int value1, int value2) throws SQLException {
        if (value1 <= value2) {
            long time = System.currentTimeMillis();
            PreparedStatement statement = connection.prepareStatement(
                    "SELECT name " +
                            "FROM cities " +
                            "WHERE population BETWEEN ? AND ?");
            statement.setInt(1, value1);
            statement.setInt(2, value2);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
            System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " ms)");
        }
        else {
            System.err.println("value1 должно быть больше либо равно value2");
        }
    }

    //JOIN вывод обменников, меняющих заданный актив
    private static void getCoinExchangers(Connection connection, String name) throws SQLException {

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT coins.name, exchangers.name " +
                        "FROM coins " +
                        "JOIN exchangers ON coins.exchangers_id = exchangers.id " +
                        "WHERE coins.name LIKE ?");
        statement.setString(1, name);
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getString(2));
        }
        System.out.println("SELECT with JOIN (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    //SELECT exchanger по id города
    private static void getExchangersId(Connection connection, int id) throws SQLException {
        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT name " +
                        "FROM exchangers " +
                        "WHERE city_id=?");
        statement.setInt(1, id);
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " ms)");
    }

    //JOIN вывод актива, цены, обменников(предоставляющих обмен), городов, в которых находятся обменники + сортировка по возрастанию стоимости актива
    private static void getCoinExchangersCities(Connection connection) throws SQLException {

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT coins.name, coins.price, exchangers.name, cities.name " +
                        "FROM coins " +
                        "JOIN exchangers ON coins.exchangers_id = exchangers.id " +
                        "JOIN cities ON exchangers.city_id = cities.id " + "ORDER BY price;");
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getDouble(2) + " | " + rs.getString(3) + " | " + rs.getString(4));
        }
        System.out.println("SELECT with JOIN (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    //JOIN вывод обменников, находящихся в заданном городе
    private static void getCoinExchangersCities(Connection connection, String name) throws SQLException {
        if (name == null || name.isBlank()) return;
        name = '%' + name + '%';

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT exchangers.name, cities.name " +
                        "FROM exchangers " +
                        "JOIN cities ON exchangers.city_id = cities.id " +
                        "WHERE cities.name LIKE ?;");
        statement.setString(1, name);
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getString(2));
        }
        System.out.println("SELECT with JOIN (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    // Добавление нового обменника
    private static void addExchanger(Connection connection, String name, int cityId)  throws SQLException {
        if (name == null || name.isBlank() || cityId < 0) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO exchangers(name, city_id) VALUES (?, ?) returning id;", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, name);
        statement.setInt(2, cityId);

        int count = statement.executeUpdate();

        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            System.out.println("Идентификатор обменника " + rs.getInt(1));
        }
        System.out.println("INSERTed " + count + " exchanger");
        getExchangers(connection);
    }

    // Удаление обменника из определённого города по введённому названию и id города
    private static void removeExchanger(Connection connection, String name, int cityId) throws SQLException {
        if (name == null || name.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement("DELETE from exchangers WHERE name LIKE ? and city_id =?;");
        statement.setString(1, name);
        statement.setInt(2, cityId);

        int count = statement.executeUpdate();
        System.out.println("DELETEd " + count + " exchangers");
        getExchangers(connection);
    }

    //Обновление населения заданного города
    private static void correctCitiesPopulation(Connection connection, String name, int population) throws SQLException {
        if (name == null || name.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE cities SET population=? WHERE name=?;");
        statement.setInt(1, population);
        statement.setString(2, name);

        int count = statement.executeUpdate();

        System.out.println("UPDATEd " + count + " cities");
        getCities(connection);
    }
}
