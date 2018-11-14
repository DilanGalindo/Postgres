
import java.sql.*;
import java.util.*;


public class test {

	public static void createTable(Connection conn) throws SQLException {
		// System.out.println("creating table");
	    String createString = 
	    	"create table benchmark ("+ 
	     	"theKey INTEGER PRIMARY KEY," +
	     	"columnA INTEGER,"+
	     	"columnB INTEGER,"+
	     	 "filler varchar(247)"+
	     	 ")" ;
	    Statement stmt = conn.createStatement();
	    stmt.executeUpdate(createString);
	    stmt.close();
	
	}
	public static double createSecondIndex1(Connection conn) throws SQLException {
//		System.out.println("creating query0");
	    String createString = 
	    	"create index part1 on benchmark (columnA)" ;
	    Statement stmt = conn.createStatement();
	   
			long startTime = System.nanoTime();
			 stmt.executeUpdate(createString);
		    stmt.close();

			long endTime = System.nanoTime();
            		long duration = (endTime - startTime);
			double seconds = (double)duration / 1000000000.0;		
	    			return seconds;

	}
		public static double createSecondIndex2(Connection conn) throws SQLException {
//		System.out.println("creating query0");
	    String createString = 
	    	"create index part1 on benchmark (columnB)" ;
	    Statement stmt = conn.createStatement();
	   
			long startTime = System.nanoTime();
			 stmt.executeUpdate(createString);
		    stmt.close();

			long endTime = System.nanoTime();
            		long duration = (endTime - startTime);
			double seconds = (double)duration / 1000000000.0;		
	    			return seconds;

	}
		public static double createSecondIndex3(Connection conn) throws SQLException {
//		System.out.println("creating query0");
	    String createString = 
	    	"create index part1 on benchmark (columnA,columnB)" ;
	    Statement stmt = conn.createStatement();
	   
			long startTime = System.nanoTime();
			 stmt.executeUpdate(createString);
		    stmt.close();

			long endTime = System.nanoTime();
            		long duration = (endTime - startTime);
			double seconds = (double)duration / 1000000000.0;		
	    			return seconds;

	}
	public static double createQuery0(Connection conn) throws SQLException {
//		System.out.println("creating query0");
	    String createString = 
	    	"select * from benchmark" ;
	    Statement stmt = conn.createStatement();
	   
			long startTime = System.nanoTime();
			 stmt.executeQuery(createString);
	
			long endTime = System.nanoTime();
            		long duration = (endTime - startTime);
			double seconds = (double)duration / 1000000000.0;		
			// System.out.println("The time from executing query0: "+duration+" in seconds: "+seconds);
	    stmt.close();
	    return seconds;
	}
	public static double createQuery1(Connection conn, int temp) throws SQLException {
//		System.out.println("creating query1");
	    String createString = 
	    	"select * from benchmark "+ 
	     	"where benchmark.columnA="+temp ;
	    Statement stmt = conn.createStatement();
	   
			long startTime = System.nanoTime();
			 
			stmt.executeQuery(createString);
	    stmt.close();
			long endTime = System.nanoTime();
            		long duration = (endTime - startTime);
			double seconds = (double)duration / 1000000000.0;	
				 return seconds;
			// System.out.println("The time from executing query1: "+duration+" in seconds: "+seconds);
	}

	public static double createQuery2(Connection conn, int temp) throws SQLException {
//		System.out.println("creating query2");
	    String createString = 
	    	"select * from benchmark "+ 
	     	"where benchmark.columnB="+temp ;
	    Statement stmt = conn.createStatement();
	   
			long startTime = System.nanoTime();
			 
			stmt.executeQuery(createString);
			long endTime = System.nanoTime();
            		long duration = (endTime - startTime);
			double seconds = (double)duration / 1000000000.0;
			// System.out.println("The time from executing query2: "+duration+" in seconds: "+seconds);
	    stmt.close();
	     return seconds;
	}

	public static double createQuery3(Connection conn, int temp) throws SQLException {
//		System.out.println("creating query3");
	    String createString = 
	    	"select * from benchmark "+ 
	     	"where benchmark.columnA="+temp+" and benchmark.columnB ="+temp ;
	    Statement stmt = conn.createStatement();
	   
			long startTime = System.nanoTime();
			 
			stmt.executeQuery(createString);
			long endTime = System.nanoTime();
            		long duration = (endTime - startTime);
			double seconds = (double)duration / 1000000000.0;	
			// System.out.println("The time from executing query3: "+duration+" in seconds: "+seconds);
	    stmt.close();
	     return seconds;
	}
	public static double insertRow(Connection conn) throws SQLException {
		// use batch insertion without autocommit to insert more rows at a time
//	    PgBulkInsert<Person> bulkInsert = new PgBulkInsert<Person>(new PersonMapping());
	   double seconds=0;
	    String sql = 
		    	"insert into benchmark (theKey,columnA,columnB,filler)"+ 
		     	"values (?,?,?,?)";
		final int batchSize=5000000;
		int count=0;
		List<Integer> list = new ArrayList<>();
		try {
			conn.setAutoCommit(false);
			// System.out.println("inserting row");
		Random rand = new Random();	
			PreparedStatement ps=conn.prepareStatement(sql);
			for(Integer k=0;k<batchSize;k++)
			{
			// if(k%2==0)
			list.add(k);
			// else
			// list.add(k-1,k);

			}
			long startTime = System.nanoTime();
			for(int j=0;j<5000;j++)
			{
				for(int i=0;i<1000;i++ )
				{
				int tempA=rand.nextInt(50000)+1;
				int tempB=rand.nextInt(50000)+1;
				ps.setInt(1, list.get(i+j*1000));
				ps.setInt(2, tempA);//Make it Random
				ps.setInt(3, tempB);// Make it Random
				ps.setString(4, tempA+tempB+"H");//Make it Random
				ps.addBatch();

				}	
			ps.executeBatch();
			}
			ps.executeBatch();
//			conn.commit();
//	    		ps.close();
			long endTime = System.nanoTime();
            		long duration = (endTime - startTime);
			 seconds = (double)duration / 1000000000.0;	
			// System.out.println("The time from inserting all the queries: "+duration+" in seconds: "+seconds);
	    		ps.close();
		//ps.setString(1,"SELECT * FROM benchmark");

		}
		catch (SQLException ex)
			{
	           System.out.println(ex.getMessage());
	            // roll back the transaction
	            System.out.println("Rolling back the transaction...");
	            try {
	                if (conn != null) {
	                    conn.rollback();
	                }
	            } catch (SQLException e) {
	                System.out.println(e.getMessage());
	            }
			}finally {
// 				conn.close();
			}
			 return seconds;
		
	}


	public static void printTable(Connection conn) throws SQLException {
		System.out.println("printing table");
	    String selectString = 
	    	"select * from benchmark";
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(selectString);
		while (rs.next()) {
	    	System.out.println(rs.getString(1) + "," + rs.getString(2)+","+rs.getString(3)+","+rs.getString(4));
		}
		rs.close();
	    stmt.close();
	}


	public static void dropTable(Connection conn) throws SQLException {
		// System.out.println("dropping table");
	    String dropString = 
	    	"drop table benchmark";
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(dropString);
	    stmt.close();
	}


	public static void main(String[] args) throws SQLException, ClassNotFoundException {

		System.out.println("loading driver");
		try {
		Class.forName("org.postgresql.Driver");
		} catch (Exception e) {
            System.out.println("Error: "+e.getMessage());
            return;
        }
		System.out.println("driver loaded");

		System.out.println("Connecting to DB");
		Connection conn=null;
		try {
		 conn = DriverManager.getConnection("jdbc:postgresql:postgres", "postgres", "secret");
		}
		catch(Exception e)
		{
            System.out.println(e.getMessage());

		}
		System.out.println("Connected to DB");

		try {
			// drops if there
			dropTable(conn);
		}
		catch (SQLException e) {}
 double totalSecond1=0;
 double totalSecond2=0;
 double totalSecond3=0;
 double totalSecond4=0;
 double totalSecond5=0;
 int[] temp={25000,10000,5000,15000,20000,2500,7500,12500,17500,22500};
		for(int h =0;h<10;h++)
		{
			createTable(conn);
			totalSecond1+=insertRow(conn);
		//	printTable(conn);

	 		totalSecond2+=createQuery0( conn);
	 		totalSecond3+=createQuery1( conn,temp[h]);
	 		totalSecond4+=createQuery2( conn,temp[h]);
	 		totalSecond5+=createQuery3( conn,temp[h]);
			dropTable(conn);
		}
	System.out.println("The time from inserting all the queries: "+totalSecond1/10+" in seconds: ");

	System.out.println("The time from query 0 to be executed: "+totalSecond2/10+" in seconds: ");

	System.out.println("The time from query 1 to be executed:  "+totalSecond3/10+" in seconds: ");

	System.out.println("The time from query 2 to be executed:  "+totalSecond4/10+" in seconds: ");

	System.out.println("The time from query 3 to be executed:  "+totalSecond5/10+" in seconds: ");
	
  totalSecond1=0;
  totalSecond2=0;
  totalSecond3=0;
  totalSecond4=0;
  totalSecond5=0;
  double totalSecond6=0;
  System.out.println("Second index columnA");
	System.out.println();

		for(int h =0;h<10;h++)
		{
			createTable(conn);
			totalSecond6+=createSecondIndex1(conn);
			totalSecond1+=insertRow(conn);
	 		totalSecond2+=createQuery0( conn);
	 		totalSecond3+=createQuery1( conn,temp[h]);
	 		totalSecond4+=createQuery2( conn,temp[h]);
	 		totalSecond5+=createQuery3( conn,temp[h]);
			dropTable(conn);
		}
	System.out.println("The time from loading the secondary index all: "+totalSecond6/10+" in seconds: ");
	System.out.println("The time from inserting all the queries: "+totalSecond1/10+" in seconds: ");
	System.out.println("The time from query 0 to be executed: "+totalSecond2/10+" in seconds: ");
	System.out.println("The time from query 1 to be executed:  "+totalSecond3/10+" in seconds: ");
	System.out.println("The time from query 2 to be executed:  "+totalSecond4/10+" in seconds: ");
	System.out.println("The time from query 3 to be executed:  "+totalSecond5/10+" in seconds: ");
  totalSecond1=0;
  totalSecond2=0;
  totalSecond3=0;
  totalSecond4=0;
  totalSecond5=0;
   totalSecond6=0;
     System.out.println("Second index columnB");
System.out.println();

		for(int h =0;h<10;h++)
		{
			createTable(conn);
			totalSecond6+=createSecondIndex2(conn);
			totalSecond1+=insertRow(conn);
	 		totalSecond2+=createQuery0( conn);
	 		totalSecond3+=createQuery1( conn,temp[h]);
	 		totalSecond4+=createQuery2( conn,temp[h]);
	 		totalSecond5+=createQuery3( conn,temp[h]);
			dropTable(conn);
		}
	System.out.println("The time from loading the secondary index all: "+totalSecond6/10+" in seconds: ");
	System.out.println("The time from inserting all the queries: "+totalSecond1/10+" in seconds: ");
	System.out.println("The time from query 0 to be executed: "+totalSecond2/10+" in seconds: ");
	System.out.println("The time from query 1 to be executed:  "+totalSecond3/10+" in seconds: ");
	System.out.println("The time from query 2 to be executed:  "+totalSecond4/10+" in seconds: ");
	System.out.println("The time from query 3 to be executed:  "+totalSecond5/10+" in seconds: ");

  totalSecond1=0;
  totalSecond2=0;
  totalSecond3=0;
  totalSecond4=0;
  totalSecond5=0;
   totalSecond6=0;
     System.out.println("Second index columnA and columnB");
System.out.println();
		for(int h =0;h<10;h++)
		{
			createTable(conn);
			totalSecond6+=createSecondIndex3(conn);
			totalSecond1+=insertRow(conn);
	 		totalSecond2+=createQuery0( conn);
	 		totalSecond3+=createQuery1( conn,temp[h]);
	 		totalSecond4+=createQuery2( conn,temp[h]);
	 		totalSecond5+=createQuery3( conn,temp[h]);
			dropTable(conn);
		}
	System.out.println("The time from loading the secondary index all: "+totalSecond6/10+" in seconds: ");
	System.out.println("The time from inserting all the queries: "+totalSecond1/10+" in seconds: ");
	System.out.println("The time from query 0 to be executed: "+totalSecond2/10+" in seconds: ");
	System.out.println("The time from query 1 to be executed:  "+totalSecond3/10+" in seconds: ");
	System.out.println("The time from query 2 to be executed:  "+totalSecond4/10+" in seconds: ");
	System.out.println("The time from query 3 to be executed:  "+totalSecond5/10+" in seconds: ");

	}
}

