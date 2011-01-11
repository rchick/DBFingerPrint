package com.rogerchick.tools.dbfingerprint;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabaseFingerPrintGenerator
{
	private String getOracleVersion = "select * from v$version";

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		//long appStart = System.currentTimeMillis ();
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

		//long appEnd = System.currentTimeMillis ();

		//System.err.println ("TOTAL TIME (MS) = " + (appEnd - appStart));
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
		//long appStart = System.currentTimeMillis ();

		int totalViewCount = 0;
		PreparedStatement statment = null;
		try
		{
			statment = conn.prepareStatement ("select count(view_name) from all_views");
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
//		long appEnd = System.currentTimeMillis ();

		//System.err.println ("getViewCount (MS) = " + (appEnd - appStart));
		System.out.print('.');

		return totalViewCount;

	}

	private int getColumnCount(Connection conn)
	{
//		long appStart = System.currentTimeMillis ();

		int totalColumnCount = 0;
		PreparedStatement statment = null;

		try
		{
			statment = conn.prepareStatement ("select count(column_name) from all_tab_columns");
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

//		long appEnd = System.currentTimeMillis ();

		//System.err.println ("getColumnCount (MS) = " + (appEnd - appStart));
		System.out.print('.');

		return totalColumnCount;
	}

	private int getIndexCount(Connection conn)
	{
//		long appStart = System.currentTimeMillis ();

		int totalIndexCount = 0;
		PreparedStatement statment = null;
		try
		{
			statment = conn.prepareStatement ("select count(index_name) from all_indexes");
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

//		long appEnd = System.currentTimeMillis ();

		//System.err.println ("getIndexCount (MS) = " + (appEnd - appStart));
		System.out.print('.');

		return totalIndexCount;
	}

	private int getTableCount(Connection conn)
	{
//		long appStart = System.currentTimeMillis ();
		PreparedStatement statment = null;
		int totalTableCount = 0;
		
		try
		{
			statment = conn.prepareStatement ("SELECT COUNT(TABLE_NAME) FROM ALL_TABLES");
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


	//	long appEnd = System.currentTimeMillis ();

		//System.err.println ("getTableCount (MS) = " + (appEnd - appStart));
		System.out.print('.');
		
		return totalTableCount;
	}

	private String getCharacterSet(Connection conn)
	{
//		long appStart = System.currentTimeMillis ();
		PreparedStatement statment = null;
		String characterSet = "";
		
		try
		{
			statment = conn.prepareStatement ("select value from nls_database_parameters where parameter='NLS_CHARACTERSET'");
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


//		long appEnd = System.currentTimeMillis ();

		//System.err.println ("getTableCount (MS) = " + (appEnd - appStart));
		System.out.print('.');
		
		return characterSet;
	}

	
	private void populateTableList(Connection conn)
	{
		

	}

	private String getMajorVersion(Connection conn)
	{
//		long appStart = System.currentTimeMillis ();

		PreparedStatement statment = null;
		try
		{
			statment = conn.prepareStatement (getOracleVersion);
			ResultSet rs = statment.executeQuery ();

//			long appEnd = System.currentTimeMillis ();

			//System.err.println ("getMajorVersion (MS) = " + (appEnd - appStart));
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
