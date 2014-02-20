package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * The class responsible for handling database operations.
 * 
 */
public class DatabaseAccess {

	private Connection connect = null;
	private static final Logger LOGGER = Logger.getLogger(DatabaseAccess.class);
	private final static String DRIVER = "org.postgresql.Driver";

	/**
	 * 
	 * @param connectionProperties
	 * @return
	 */
	public void openDBConnection(final Properties connectionProperties) {

		String url = connectionProperties.getProperty("database.url");
		String dbName = connectionProperties.getProperty("database.name");
		String userName = connectionProperties.getProperty("database.username");
		String password = connectionProperties.getProperty("database.password");
		try {
			Class.forName(DRIVER).newInstance();

			connect = (Connection) DriverManager.getConnection(url + dbName, userName, password);

		} catch (Exception e) {
			LOGGER.error("Unable to connect to database. Please check the settings", e);
		}

	}

	/**
	 * Open a database connection that is user specific. Used to store the
	 * results of the simulation to the local database of the submittee.
	 * 
	 * @param url
	 * @param userName
	 * @param password
	 */
	public void openDBConnection(String url, String userName, String password) {
		try {
			Class.forName(DRIVER).newInstance();

			connect = (Connection) DriverManager.getConnection(url, userName, password);

		} catch (Exception e) {
			LOGGER.error("Unable to connect to database. Please check the settings", e);
		}

	}

	/**
	 * Return the result set for the SELECT query.
	 * 
	 * @param queryString
	 * @return
	 */
	public ResultSet retrieveQueryResult(String queryString) {
		LOGGER.info(queryString);

		ResultSet resultSet = null;
		try {
			PreparedStatement preparedStatement = (PreparedStatement) connect
					.prepareStatement(queryString);
			// preparedStatement.setFetchSize(Integer.MIN_VALUE);
			resultSet = preparedStatement.executeQuery();
		} catch (SQLException e) {
			LOGGER.error(queryString);
			LOGGER.error("Error retrieving result set. Please check the logged query", e);

		}
		return resultSet;
	}

	/**
	 * Call for DDL statements i.e. DELETE, UPDATE and INSERT
	 * 
	 * @param queryString
	 */
	public void executeUpdate(String queryString) {
		try {
			PreparedStatement preparedStatement = (PreparedStatement) connect
					.prepareStatement(queryString);
			preparedStatement.execute();

		} catch (SQLException e) {
			LOGGER.error("Error while executing the DDL statement\n" + queryString, e);

		}
	}

	/**
	 * Close the database connection.
	 * 
	 * @throws SQLException
	 */

	public void closeConnection() throws SQLException {
		connect.close();
	}

}
