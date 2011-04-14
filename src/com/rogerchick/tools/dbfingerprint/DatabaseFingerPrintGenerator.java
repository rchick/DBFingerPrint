package com.rogerchick.tools.dbfingerprint;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabaseFingerPrintGenerator
{
	private static final String GET_ORACLE_VERSION = "SELECT * FROM V$VERSION";
	private static final String GET_ORACLE_CHARACTER_SET = "SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER='NLS_CHARACTERSET'";
	private static final String GET_TABLE_COUNT = "SELECT COUNT(TABLE_NAME) FROM ALL_TABLES";
	private static final String GET_INDEX_COUNT = "SELECT COUNT(INDEX_NAME) FROM ALL_INDEXES";
	private static final String GET_VIEW_COUNT = "SELECT COUNT(VIEW_NAME) FROM ALL_VIEWS";
	private static final String GET_COLUMN_COUNT = "SELECT COUNT(COLUMN_NAME) FROM ALL_TAB_COLUMNS";

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		int num = args.length;

		if (num < 3)
		{
			System.out
			        .println ("Syntax: java -classpath <oracle-driver> -jar DBFingerPrint.jar <JDBC URL> <username> <password>");
			System.out
			        .println ("<oracle-driver> The full path to the Oracle driver jar file - either classes12.zip or ojdbc14.jar will suffice");
			System.out.println ("<JDBC URL> db schema full JDBC URL.");
			System.out.println ("<username> db schema Username.");
			System.out.println ("<password> db schema Password.");

			System.exit (1);
		}

		DatabaseFingerPrintGenerator dbFingerPrint = new DatabaseFingerPrintGenerator(); 
		dbFingerPrint.getDatabaseFingerprint (args[0], args[1], args[2]);

		System.out.print('.');
	}

	public String getDatabaseFingerprint(String url, String username, String password)
	{
		String result = null;
		try
		{
			Class.forName ("oracle.jdbc.driver.OracleDriver");
		}
		catch (ClassNotFoundException e1)
		{
			e1.printStackTrace ();
		}

		String version = "";
		int tableCount = -1;
		int columnCount = -1;
		int indexCount = -1;
		int viewCount = -1;
		String characterSet = "";
		
		Connection conn = null;
		try
		{
			conn = DriverManager.getConnection (url, username, password);

			version = getMajorVersion (conn);

			tableCount = getTableCount (conn);

			columnCount = getColumnCount (conn);

			indexCount = getIndexCount (conn);

			viewCount = getViewCount (conn);

			characterSet = getCharacterSet(conn);
			
			result = "\"" + version + "\"." + tableCount + "." + columnCount + "." + indexCount + "."
			        + viewCount + ".\"" + characterSet + "\"";
			
			System.out.println ("\nversion.tableCount.columnCount.indexCount.viewCount.characterSet");
			System.out.println (result);
		}
		catch (SQLException e)
		{
			e.printStackTrace ();
		}
		finally
		{
			try
			{
				conn.close ();
			}
			catch (SQLException e)
			{
				e.printStackTrace ();
			}
		}

		return result;
	}

	private int getViewCount(Connection conn)
	{
		int totalViewCount = 0;
		PreparedStatement statment = null;
		try
		{
			statment = conn.prepareStatement (DatabaseFingerPrintGenerator.GET_VIEW_COUNT);
			ResultSet rs = statment.executeQuery ();

			rs.next ();

			totalViewCount = rs.getInt ("count(view_name)");

			rs.close ();

		}
		catch (SQLException e)
		{
			e.printStackTrace ();
		}
		finally
		{
			try
			{
				statment.close ();
			}
			catch (SQLException e)
			{
				e.printStackTrace ();
			}
		}
		System.out.print('.');

		return totalViewCount;
	}

	private int getColumnCount(Connection conn)
	{
		int totalColumnCount = 0;
		PreparedStatement statment = null;

		try
		{
			statment = conn.prepareStatement (DatabaseFingerPrintGenerator.GET_COLUMN_COUNT);
			ResultSet rs = statment.executeQuery ();

			rs.next ();

			totalColumnCount = rs.getInt ("count(column_name)");

			rs.close ();
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}

		try
		{
			statment.close ();
		}
		catch (SQLException e)
		{
			e.printStackTrace ();
		}

		System.out.print('.');

		return totalColumnCount;
	}

	private int getIndexCount(Connection conn)
	{
		int totalIndexCount = 0;
		PreparedStatement statment = null;
		try
		{
			statment = conn.prepareStatement (DatabaseFingerPrintGenerator.GET_INDEX_COUNT);
			ResultSet rs = statment.executeQuery ();

			rs.next ();
			totalIndexCount = rs.getInt ("count(index_name)");

			rs.close ();
		}
		catch (SQLException e)
		{
			e.printStackTrace ();
		}
		finally
		{
			try
			{
				statment.close ();
			}
			catch (SQLException e)
			{
				e.printStackTrace ();
			}
		}

		System.out.print('.');

		return totalIndexCount;
	}

	private int getTableCount(Connection conn)
	{
		PreparedStatement statment = null;
		int totalTableCount = 0;
		
		try
		{
			statment = conn.prepareStatement (DatabaseFingerPrintGenerator.GET_TABLE_COUNT);
			ResultSet rs = statment.executeQuery ();

			rs.next ();
			totalTableCount = rs.getInt ("COUNT(TABLE_NAME)");

			rs.close ();
		}
		catch (SQLException e)
		{
			e.printStackTrace ();
		}
		finally
		{
			try
			{
				statment.close ();
			}
			catch (SQLException e)
			{
				e.printStackTrace ();
			}
		}

		System.out.print('.');
		
		return totalTableCount;
	}

	private String getCharacterSet(Connection conn)
	{
		PreparedStatement statment = null;
		String characterSet = "";
		
		try
		{
			statment = conn.prepareStatement (DatabaseFingerPrintGenerator.GET_ORACLE_CHARACTER_SET);
			ResultSet rs = statment.executeQuery ();

			rs.next ();
			characterSet = rs.getString (1);

			rs.close ();
		}
		catch (SQLException e)
		{
			e.printStackTrace ();
		}
		finally
		{
			try
			{
				statment.close ();
			}
			catch (SQLException e)
			{
				e.printStackTrace ();
			}
		}

		System.out.print('.');
		
		return characterSet;
	}

	
	private void populateTableList(Connection conn)
	{
		

	}

	private String getMajorVersion(Connection conn)
	{
		PreparedStatement statment = null;
		try
		{
			statment = conn.prepareStatement (DatabaseFingerPrintGenerator.GET_ORACLE_VERSION);
			ResultSet rs = statment.executeQuery ();

			System.out.print('.');
			
			return getVersionResultFromResultSet (rs);
		}
		catch (SQLException e)
		{
			e.printStackTrace ();
		}
		finally
		{
			try
			{
				statment.close ();
			}
			catch (SQLException e)
			{
				e.printStackTrace ();
			}
		}

		return null;
	}

	private String getVersionResultFromResultSet(ResultSet rs) throws SQLException
	{
		rs.next ();
		String major = rs.getString (1);
		String firstWord = major.substring (0, major.indexOf (' '));

		rs.close ();
		return major;
	}
}
