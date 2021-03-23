package database;


//import com.thedeanda.lorem.Lorem;
//import com.thedeanda.lorem.LoremIpsum;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.h2.tools.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static java.lang.System.exit;

public class MyDb_sample {
    private static final Logger logger = LoggerFactory.getLogger(MyDb_sample.class);
    private static final String DB_URL = "jdbc:h2:~/sample";
    private static final String DB_USERNAME = "sa";
    private static final String DB_PASSWORD = "";
    private final Properties sqlCommands = new Properties();
    //private final Lorem generator = LoremIpsum.getInstance();
    private HikariDataSource hikariDatasource;

    private Server h2Server, webServer;

    public static void main(String[] args) {
        MyDb_sample demo = new MyDb_sample();
        demo.loadSqlCommands();
        demo.startH2Server();
        demo.initiateConnectionPooling();

        if (demo.createTable()){
            demo.insertData();
        }
        demo.selectData();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> demo.stopH2Server()));
    }


    private void loadSqlCommands(){
        try (InputStream inputStream = MyDb_sample.class.getClassLoader().getResourceAsStream("sql.properties")){
            if (inputStream == null){
                logger.error("Unable to load SQL commands.");
                exit(-1);
            }

            sqlCommands.load(inputStream);
        } catch (IOException e) {
            logger.error("Error while loading SQL commands.",e);
        }
    }

    private void startH2Server(){
        try{
            h2Server = Server.createTcpServer("-tcpAllowOthers","-tcpDaemon");
            h2Server.start();
            webServer = Server.createWebServer("-webAllowOthers","-webDaemon");
            webServer.start();
            logger.info("H2 Database server is now accepting connections.");

        }catch (SQLException throwables) {
            logger.error("Unable to start H2 database server",throwables);
            exit(-1);
        }
        logger.info("H2 server has started with status '{}'.",h2Server.getStatus());
    }

    private void initiateConnectionPooling(){
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("org.h2.Driver");
        config.setJdbcUrl(DB_URL);
        config.setUsername(DB_USERNAME);
        config.setPassword(DB_PASSWORD);

        config.setConnectionTimeout(10000);
        config.setIdleTimeout(60000);
        config.setMaxLifetime(1800000);
        config.setMinimumIdle(1);
        config.setMaxLifetime(5);
        config.setAutoCommit(true);

        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtsCacheSize","500");
        hikariDatasource = new HikariDataSource(config);
    }

    private boolean createTable(){
        try(Statement statement = hikariDatasource.getConnection().createStatement()){
            int resultRows = statement.executeUpdate(sqlCommands.getProperty("create.table.001"));
            logger.debug("Statement returned {}.",resultRows);
            return true;
        } catch (SQLException throwables) {
            logger.warn("Unable to create target database table. It already exists.");
            return false;
        }
    }

    private void insertData(){
        try(Statement statement = hikariDatasource.getConnection().createStatement()){
            int resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.001"));
            logger.debug("Statement returned {}.",resultRows);
            resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.002"));
            logger.debug("Statement returned {}.",resultRows);
            resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.003"));
            logger.debug("Statement returned {}.",resultRows);
            resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.004"));
            logger.debug("Statement returned {}.",resultRows);
            resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.005"));
            logger.debug("Statement returned {}.",resultRows);

        } catch (SQLException throwables) {
            logger.error("Error occurred while inserting data.",throwables);
        }
    }
    private void selectData(){
        try (Statement statement = hikariDatasource.getConnection().createStatement();
             ResultSet resultSet = statement.executeQuery(sqlCommands.getProperty("select.table.001"))){

            while (resultSet.next()){
                //@formatter:off
                logger.info("id:{}, firstName:{}, lastName:{}, age:{}.",
                        resultSet.getLong("ID"),
                        resultSet.getString("FIRSTNAME"),
                        resultSet.getString("LASTNAME"),
                        resultSet.getInt("AGE"));
                //@formatter:on
            }
        } catch (SQLException throwables) {
            logger.error("Error occurred while retrieving data", throwables);
        }
    }
    private void stopH2Server(){
        if (h2Server == null || webServer == null){
            return;
        }
        if (h2Server.isRunning(true)){
            h2Server.stop();
            h2Server.shutdown();
        }
        if(webServer.isRunning(true)){
            webServer.stop();
            webServer.shutdown();
        }
        logger.info("H2 Database server has been shutdown.");
    }
}
