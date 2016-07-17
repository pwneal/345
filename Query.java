import java.util.Properties;;;;;;;;;;;;;;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.Stack;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;

/**
 * Runs queries against a back-end database
 */
public class Query {
	int numOfCusts = 10000; //480189
	int numOfFilms = 10000; //17770; lower numbers to test
	int[][]ratings = null;
	String[]movieTitles= new String[17770];
	
    private static Properties configProps = new Properties();

    private static String imdbUrl;
    private static String customerUrl;

    private static String postgreSQLDriver;
    private static String postgreSQLUser;
    private static String postgreSQLPassword;

    // DB Connection
    private Connection _imdb;
    private Connection _customer_db;

    // Canned queries

    private String _search_sql = "SELECT * FROM movie WHERE name like ? ORDER BY id";
    private PreparedStatement _search_statement;

    private String _director_mid_sql = "SELECT y.* "
                     + "FROM movie_directors x, directors y "
                     + "WHERE x.mid = ? and x.did = y.id";
    private PreparedStatement _director_mid_statement;
    
    // added for search method
	private String _actor_mid_sql = "SELECT actor.fname, actor.lname "
									+"FROM movie, casts, actor "
									+"WHERE movie.id=casts.mid AND casts.pid=actor.id AND movie.id=?";
	private PreparedStatement _actor_mid_statement;
	
	private String _availability_sql = "SELECT cid FROM movies WHERE mid = ?";
	private PreparedStatement _availability_statement;
	
	//for list_plans:
	private String _list_plans_sql = "SELECT * FROM Plan";
	private PreparedStatement _list_plans_statement;
	
	//for choose_plan:
	private String _update_plan_sql = "UPDATE customers SET plid = ? WHERE cid = ?";
	private PreparedStatement _update_plan_statement;
	private String _plan_limit_sql = "SELECT name, movieLimit FROM Plan WHERE plid = ?";
	private PreparedStatement _plan_limit_statement;
	private String _current_plan_sql = "SELECT Plan.plid, Plan.name, Plan.movieLimit FROM Plan, Customers WHERE Plan.plid = Customers.plid AND Customers.cid = ?";
	private PreparedStatement _current_plan_statement;
	
	//for list_user_rentals:
	private String _currently_rented_sql = "SELECT m.name FROM Movies m, rents r WHERE m.mid = r.mid AND r.cid = ?";
	private PreparedStatement _currently_rented_statement;

    private String _fast_search_director_sql = "SELECT x.mid, y.* "
                     + "FROM movie_directors x, directors y "
                     + "WHERE x.mid = ANY (?) and x.did = y.id "
                     + "ORDER BY x.mid";
    private PreparedStatement _fast_search_director_statement;

    private String _fast_search_actor_sql = "SELECT x.mid, y.* "
                     + "FROM casts x, actor y "
                     + "WHERE x.mid = ANY (?) and x.pid = y.id "
                     + "ORDER BY x.mid";
    private PreparedStatement _fast_search_actor_statement;

    /* uncomment, and edit, after your create your own customer database */
    private String _customer_login_sql = "SELECT * FROM Customers WHERE username = ? and password = ? ";
    private PreparedStatement _customer_login_statement;

    private String _begin_transaction_read_write_sql = "BEGIN TRANSACTION READ WRITE";
    private PreparedStatement _begin_transaction_read_write_statement;

    private String _personal_data_sql = "select c.name, (movielimit - count(Rents.*)) AS remaining "+
                                        "from customers as c INNER JOIN "+
                                        "Rents on Rents.cid=c.cid, "+
                                        "Plan "+
                                        "WHERE Plan.plid=c.plid AND "+
                                        "c.cid = ? AND dateEnd IS NULL "+
                                        "GROUP BY c.name, movielimit;";
    private PreparedStatement _personal_data_statement;

    private String _commit_transaction_sql = "COMMIT TRANSACTION";
    private PreparedStatement _commit_transaction_statement;

    private String _rollback_transaction_sql = "ROLLBACK TRANSACTION";
    private PreparedStatement _rollback_transaction_statement;
    
    private String _rent_movie_sql = "insert into rents values(?, ?, current_timestamp, NULL);";
    private PreparedStatement _rent_movie_statement;

    private String _add_cid_movie_sql = "update movies set cid = ? where mid = ?;";
    private PreparedStatement _add_cid_movie_statement;

    private String _return_movie_sql = "update rents set dateend = current_timestamp where mid = ? and cid = ?;";
    private PreparedStatement _return_movie_statement;

    private String _remove_cid_movie_sql = "update movies set cid = NULL where mid = ?;";
    private PreparedStatement _remove_cid_movie_statement;
	
	private String _insert_rating = "INSERT INTO Ratings(cid, mid, rating) VALUES(?,?,?)";
	private PreparedStatement _insert_rating_statement;
	
	private String _get_user_ratings_sql = "select mid, rating from Ratings where cid = ?";
	private PreparedStatement _get_user_ratings_statement;

    public Query() {
    }

    /**********************************************************/
    /* Connections to postgres databases */

    public void openConnection() throws Exception {
        configProps.load(new FileInputStream("dbconn.config"));
        
        
        imdbUrl        = configProps.getProperty("imdbUrl");
        customerUrl    = configProps.getProperty("customerUrl");
        postgreSQLDriver   = configProps.getProperty("postgreSQLDriver");
        postgreSQLUser     = configProps.getProperty("postgreSQLUser");
        postgreSQLPassword = configProps.getProperty("postgreSQLPassword");


        /* load jdbc drivers */
        Class.forName(postgreSQLDriver).newInstance();

        /* open connections to TWO databases: imdb and the customer database */
        _imdb = DriverManager.getConnection(imdbUrl, // database
                postgreSQLUser, // user
                postgreSQLPassword); // password

        _customer_db = DriverManager.getConnection(customerUrl, // database
                postgreSQLUser, // user
                postgreSQLPassword); // password
        
        create_recommendation_matrix();
    }

    public void closeConnection() throws Exception {
        _imdb.close();
        _customer_db.close();
    }

    /**********************************************************/
    /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

    public void prepareStatements() throws Exception {

        _search_statement = _imdb.prepareStatement(_search_sql);
        _director_mid_statement = _imdb.prepareStatement(_director_mid_sql);
        _actor_mid_statement = _imdb.prepareStatement(_actor_mid_sql);
		_availability_statement = _customer_db.prepareStatement(_availability_sql);
        _fast_search_director_statement = _imdb.prepareStatement(_fast_search_director_sql);
        _fast_search_actor_statement = _imdb.prepareStatement(_fast_search_actor_sql);

        _customer_login_statement = _customer_db.prepareStatement(_customer_login_sql);
        _begin_transaction_read_write_statement = _customer_db.prepareStatement(_begin_transaction_read_write_sql);
        _commit_transaction_statement = _customer_db.prepareStatement(_commit_transaction_sql);
        _rollback_transaction_statement = _customer_db.prepareStatement(_rollback_transaction_sql);
        _personal_data_statement = _customer_db.prepareStatement(_personal_data_sql);
        
        _list_plans_statement = _customer_db.prepareStatement(_list_plans_sql);
		_update_plan_statement = _customer_db.prepareStatement(_update_plan_sql);
		_plan_limit_statement = _customer_db.prepareStatement(_plan_limit_sql);
		_current_plan_statement = _customer_db.prepareStatement(_current_plan_sql);
        
        _currently_rented_statement = _customer_db.prepareStatement(_currently_rented_sql);
        _rent_movie_statement = _customer_db.prepareStatement(_rent_movie_sql);
        _return_movie_statement = _customer_db.prepareStatement(_return_movie_sql);
        _add_cid_movie_statement = _customer_db.prepareStatement(_add_cid_movie_sql);
        _remove_cid_movie_statement = _customer_db.prepareStatement(_remove_cid_movie_sql);

		_insert_rating_statement = _customer_db.prepareStatement(_insert_rating);
		_get_user_ratings_statement = _customer_db.prepareStatement("_get_user_ratings_sql");
    }


    /**********************************************************/
    /* suggested helper functions  */

    public int helper_compute_remaining_rentals(int cid) throws Exception {
        /* how many movies can she/he still rent ? */
        /* you have to compute and return the difference between the customer's plan
           and the count of oustanding rentals */
        return (99);
    }

    public String helper_compute_customer_name(int cid) throws Exception {
        /* you find  the first + last name of the current customer */
        return ("JoeFirstName" + " " + "JoeLastName");

    }

    public boolean helper_check_plan(int plan_id) throws Exception {
        /* is plan_id a valid plan id ?  you have to figure out */
        return true;
    }

    public boolean helper_check_movie(int mid) throws Exception {
        /* is mid a valid movie id ? you have to figure out  */
        return true;
    }

    private int helper_who_has_this_movie(int mid) throws Exception {
        /* find the customer id (cid) of whoever currently rents the movie mid; return -1 if none */
        return (77);
    }

    /**********************************************************/
    /* login transaction: invoked only once, when the app is started  */
    public int transaction_login(String name, String password) throws Exception {
        /* authenticates the user, and returns the user id, or -1 if authentication fails */
        int cid;

        _customer_login_statement.clearParameters();
        _customer_login_statement.setString(1,name);
        _customer_login_statement.setString(2,password);
        ResultSet cid_set = _customer_login_statement.executeQuery();
        if (cid_set.next()) cid = cid_set.getInt(1);
        else cid = -1;
        return(cid);
    }

    public void transaction_personal_data(int cid) throws Exception {
        /* println the customer's personal data: name, and plan number */
                
        String Greeting = "";
        
        _personal_data_statement.clearParameters();
        _personal_data_statement.setInt(1, cid);
        ResultSet personal_data_set = _personal_data_statement.executeQuery();
        if (personal_data_set.next()) {
            String displayName;
            int remainingRentals;
            displayName = personal_data_set.getString(1);
            remainingRentals = personal_data_set.getInt(2);
            Greeting = "Hello "+displayName+" you have "+
                        ((remainingRentals > 8) ? "infinite" : remainingRentals)+
                        " rentals remaining.";
        }
        System.out.println(Greeting);
    }


    /**********************************************************/
    /* main functions in this project: */
	
	public void transaction_rate(int cid, String movie_title)
			throws Exception {
			/* allows a user to search a movie by name, will generate a list of movies  
				for them to choose from, and then accept a rating 1-5 */
		_search_statement.clearParameters();
        _search_statement.setString(1, '%' + movie_title + '%');
        ResultSet movie_set = _search_statement.executeQuery();
		Stack<Integer> MovieStack = new Stack<Integer>();
		int mid = -1;
		if (movie_set.next()){
			// print out results for the search
			Scanner inputScanner = (new Scanner(System.in));
			do  {
				int renter = -1;
				mid = movie_set.getInt(1);
				MovieStack.push(mid);
				System.out.format("%d\t\"%s\" (%s)%n"
						,MovieStack.size(), movie_set.getString(2), movie_set.getString(3));
			}	while (movie_set.next());
			System.out.print("Enter a number option to select one of the movies listed");
			int intput =  -1; // LOL
			while ((intput < 0) || (intput >= MovieStack.size())){ // until given valid input, get input
				try{
					String num = inputScanner.nextLine();
					System.out.println(num);
					intput = Integer.parseInt(num);
					if ((intput < 0) || (intput >= MovieStack.size())) // invalid input, prompt the user and try again
						System.out.println("That selection is invalid.\nPlease enter a number option listed above.");
					
				} catch (Exception e){ // unexpected behavior. prompt the use and try again
					e.printStackTrace();
					System.out.println("Didn't recognize your selection.\nPlease enter a number option listed above");
					continue;
				}
			}
			// get the mid to match the selection
			while (MovieStack.size() != intput){
				MovieStack.pop();
			}
			mid = MovieStack.pop();
			// valid selection has been made. Get rating.
			int rating=0;
			System.out.println("Enter a rating (1-5)");
			while ((rating < 1) || (rating > 5)){
				try{
					rating = Integer.parseInt(inputScanner.nextLine());
					if ((rating < 0)&&(rating > 5))
						System.out.println("That selection is invalid.\nPlease enter 1-5");
				} catch (Exception E){
					System.out.println("Didn't recognize your selection.\nPlease enter 1-5");
					continue;
				}
			}
			// Rating and movie have been identified. Save the rating.
			_insert_rating_statement.clearParameters();
			_insert_rating_statement.setInt(1,cid);
			_insert_rating_statement.setInt(2,mid);
			_insert_rating_statement.setInt(3, rating);
			_insert_rating_statement.executeUpdate();
		} else {
			System.out.println("No items found matching the search title : \""+movie_title+"\"");
		}
	}

    public void transaction_search(int cid, String movie_title)
            throws Exception {
        /* searches for movies with matching titles: SELECT * FROM movie WHERE name LIKE movie_title */
        /* prints the movies, directors, actors, and the availability status:
           AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT */

        /* set the first (and single) '?' parameter */
        _search_statement.clearParameters();
        _search_statement.setString(1, '%' + movie_title + '%');

        ResultSet movie_set = _search_statement.executeQuery();
        while (movie_set.next()) {
            int renter = -1;
            int mid = movie_set.getInt(1);
            System.out.println("ID: " + mid + " NAME: "
                    + movie_set.getString(2) + " YEAR: "
                    + movie_set.getString(3));
            /* do a dependent join with directors */
            _director_mid_statement.clearParameters();
            _director_mid_statement.setInt(1, mid);
            ResultSet director_set = _director_mid_statement.executeQuery();
            while (director_set.next()) {
                System.out.println("\t\tDirector: " + director_set.getString(3)
                        + " " + director_set.getString(2));
            }
            director_set.close();
            /* now you need to retrieve the actors, in the same manner */
			_actor_mid_statement.clearParameters();
			_actor_mid_statement.setInt(1, mid);
			ResultSet actor_set = _actor_mid_statement.executeQuery();
			while (actor_set.next()) {
				System.out.println("\t\tActor: " + actor_set.getString(2) + " " + actor_set.getString(1));
			}
			actor_set.close();
			
            /* then you have to find the status: of "AVAILABLE" "YOU HAVE IT", "UNAVAILABLE" */
			_availability_statement.clearParameters();
			_availability_statement.setInt(1, mid);
			ResultSet cid_set = _availability_statement.executeQuery();
			if (cid_set.next()) {
				renter = cid_set.getInt(1);
			}
			// if no results print AVAILABLE
			if (renter == 0) {
				System.out.println("\t\tAVAILABLE");
			} 
			// if non-customer print UNAVAILABLE
			else if (renter != cid) {
				System.out.println("\t\tUNAVAILABLE");
			}
			// if customer result print YOU HAVE IT
			else if (renter == cid) {
				System.out.println("\t\tYOU HAVE IT");
			}
        }
        System.out.println();
    }

    public void transaction_choose_plan(int cid, int pid) throws Exception {
        /* updates the customer's plan to pid: UPDATE customers SET plid = pid WHERE customers.cid = this.cid*/
        /* remember to enforce consistency ! */

		_begin_transaction_read_write_statement.executeUpdate();
		_personal_data_statement.clearParameters();
		_personal_data_statement.setInt(1,cid);
		ResultSet personal_data_set = _personal_data_statement.executeQuery();
		personal_data_set.next();
		
		_current_plan_statement.clearParameters();
		_current_plan_statement.setInt(1,cid);
		ResultSet current_plan = _current_plan_statement.executeQuery();
		current_plan.next();
		
		_plan_limit_statement.clearParameters();
		_plan_limit_statement.setInt(1, pid);
		ResultSet plan_limit = _plan_limit_statement.executeQuery();
		if (plan_limit.next()) {
			
			if (current_plan.getInt(1) == pid) {
				System.out.println("You are already on the "+current_plan.getString(2)+" plan.");
			}
			else if ((current_plan.getInt(3) - personal_data_set.getInt(2)) <= plan_limit.getInt(2)) {
				_update_plan_statement.clearParameters();
				_update_plan_statement.setInt(1, pid);
				_update_plan_statement.setInt(2, cid);
				_update_plan_statement.executeUpdate();
				_commit_transaction_statement.executeUpdate();
				System.out.println("You are now on the "+plan_limit.getString(1)+" plan.");
			}
			else {
				 System.out.println("You have too many movies out to switch to that plan.");
				 _rollback_transaction_statement.executeUpdate();
			}
		} else {
			System.out.println("That is not a valid plan ID.");
			_rollback_transaction_statement.executeUpdate();
		}
    }

    public void transaction_list_plans() throws Exception {
        /* println all available plans: SELECT * FROM plan */
        	_begin_transaction_read_write_statement.executeUpdate();
		ResultSet plans = _list_plans_statement.executeQuery();
		_commit_transaction_statement.executeUpdate();
		while (plans.next()) {
			System.out.println("NAME: "+plans.getString(1)+"\tID: "+plans.getString(2)+"\tCOST: "+String.format("%.2f",plans.getFloat(3))+"\tRENTAL LIMIT: "+plans.getString(4));
		}
    }
    
    public void transaction_list_user_rentals(int cid) throws Exception {
        /* println all movies rented by the current user*/
		_begin_transaction_read_write_statement.executeUpdate();
		_currently_rented_statement.clearParameters();
		_currently_rented_statement.setInt(1, cid);
		ResultSet rented = _currently_rented_statement.executeQuery();
		_commit_transaction_statement.executeUpdate();
		System.out.println("You are currently renting:");
		while (rented.next()) {
			System.out.println(rented.getString(1));
		}
    }

    public void transaction_rent(int cid, int mid) throws Exception {
    	_begin_transaction_read_write_statement.executeUpdate();
        _personal_data_statement.clearParameters();
        _personal_data_statement.setInt(1, cid);
        ResultSet personal_data_set = _personal_data_statement.executeQuery();

        _availability_statement.clearParameters();
        _availability_statement.setInt(1, mid);
        ResultSet available = _availability_statement.executeQuery();

        if (personal_data_set.next()) {
            if (available.next()) {
                int remainingRentals = personal_data_set.getInt(2);
                if((remainingRentals > 0) && (available.getInt(1) == 0)){
                     _rent_movie_statement.clearParameters();
                     _add_cid_movie_statement.clearParameters();

                     _rent_movie_statement.setInt(1, cid);
                     _rent_movie_statement.setInt(2, mid);
                     _rent_movie_statement.executeUpdate();

                     _add_cid_movie_statement.setInt(1, cid);
                     _add_cid_movie_statement.setInt(2, mid);
                     _add_cid_movie_statement.executeUpdate();
                    _commit_transaction_statement.executeUpdate();
                }
                else{
                System.out.println("You have already rented the max amount of movies for your plan or you are renting a movie that you already have or is checked out to someone else.");
                _rollback_transaction_statement.executeUpdate();
                }
            }
        }
        /* rend the movie mid to the customer cid */
        /* remember to enforce consistency ! */
    }

    public void transaction_return(int cid, int mid) throws Exception {
    	_begin_transaction_read_write_statement.executeUpdate();
        _availability_statement.clearParameters();
        _availability_statement.setInt(1, mid);
        ResultSet available = _availability_statement.executeQuery();
        
        if (available.next()) {
            if (available.getInt(1) == cid){
                _return_movie_statement.clearParameters();
                _return_movie_statement.setInt(1, mid);
                _return_movie_statement.setInt(2, cid);
                _return_movie_statement.executeUpdate();
        
                _remove_cid_movie_statement.clearParameters();
                _remove_cid_movie_statement.setInt(1, mid);
                _remove_cid_movie_statement.executeUpdate();
                _commit_transaction_statement.executeUpdate();
            }
        else{
            System.out.println("You did not rent this movie");
            _rollback_transaction_statement.executeUpdate();
            }
        }
        /* return the movie mid by the customer cid */
    }

    public void transaction_fast_search(int cid, String movie_title)
            throws Exception {
        _search_statement.clearParameters();
        _search_statement.setString(1, '%' + movie_title + '%');

        ResultSet movie_set = _search_statement.executeQuery();
        ArrayList <Integer> mid_result = new ArrayList<Integer>();
        ArrayList <String> string_result = new ArrayList<String>();
        while (movie_set.next()) {
            mid_result.add(movie_set.getInt(1));
            string_result.add(movie_set.getString(2)+"|"+movie_set.getString(3));
        }
        Integer[] data = mid_result.toArray(new Integer[mid_result.size()]);
        Array mid_array = _imdb.createArrayOf("integer", data);

        _fast_search_director_statement.clearParameters();
        _fast_search_director_statement.setArray(1, mid_array);
        _fast_search_actor_statement.clearParameters();
        _fast_search_actor_statement.setArray(1, mid_array);

        ResultSet director_set = _fast_search_director_statement.executeQuery();
        ResultSet actor_set = _fast_search_actor_statement.executeQuery();

        director_set.next();
        actor_set.next();

        for (int x=0; x<mid_result.size(); x++){
            int mid = mid_result.get(x);
            String info = string_result.get(x);
            System.out.println("ID: " + mid + " NAME: "
                    + info.substring(0, info.indexOf("|")) + " YEAR: "
                    + info.substring(info.indexOf("|")+1, info.length()));
            do{
                int dir_mid = director_set.getInt(1);
                if (dir_mid != mid)
                    break;
                System.out.println("\t\tDirector: " + director_set.getString(3)
                        + " " + director_set.getString(4));
            }while(director_set.next());
            do{
                int act_mid = actor_set.getInt(1);
                if (act_mid != mid)
                    break;
                System.out.println("\t\tActor: " + actor_set.getString(3)
                        + " " + actor_set.getString(4));
            }while(actor_set.next());
        }
    }
    
    public void create_recommendation_matrix() throws IOException{
    	ratings = new int[numOfFilms+1][numOfCusts];
    	BufferedReader in = null;
    	String root = "C:/Users/Patrick/Downloads/download/";
    	String next = null;
    	
    	for(int x=0; x<numOfFilms; x++){
            int xID = x+1;
    		String xString = "" + xID;
    		String file = ("0000000" + xString).substring(xString.length());
    		try{
        		in = new BufferedReader(new FileReader(root + "training_set/mv_" + file + ".txt"));
                in.readLine(); //one throwaway for the header line
        		while((next = in.readLine())!=null){
        			String[]data = next.split(",");
        			int custID = Integer.parseInt(data[0]);
        			int hashID = custID%numOfCusts;
        			int timer = 0;
        			while(ratings[numOfFilms][hashID]!=custID && ratings[numOfFilms][hashID]!=0 && timer<20){
        				hashID++;
        				hashID%=numOfCusts;
        				timer++;
        			}
        			if(timer<20){
            			ratings[numOfFilms][hashID]=custID;
            			ratings[x][hashID]=Integer.parseInt(data[1]);	
        			}
        		}
    		}  
    		finally{
    			if(in!=null)
    				in.close();
    		}
    	}
    	
    	in = new BufferedReader(new FileReader(root +"movie_titles.txt"));
    	while((next = in.readLine())!=null){
    		String[]data = next.split(",");
    		int movieID = Integer.parseInt(data[0]);
    		movieTitles[movieID-1]= data[2];
    	}
    }
    
    public void transaction_recommend(int cid) throws SQLException{
        ArrayList <Integer> userRatingsList = new ArrayList<Integer>();
    	double[][]predictedRatings = new double[numOfFilms][2];
    	
    	/*get our user ratings here
		_get_user_ratings_statement.clearParameters();
		_get_user_ratings_statement.setInt(1, cid);
		ResultSet SQLratings = _get_user_ratings_statement.executeQuery();
		while (SQLratings.next()) {
			userRatingsList.add((SQLratings.getInt(1)*10)+SQLratings.getInt(2));
		}
        Integer[] userRatings = userRatingsList.toArray(new Integer[userRatingsList.size()]);

        //then they need to be turned into Netflix IDs
*/
    	Integer[] userRatings = {55825,57055,82925,86875,96285,98645}; // for test purposes
    	
    	for(int x=0; x<numOfFilms; x++){
    		int totalInstances=0;
    		for(int rating:userRatings){
    			int id = rating/10;
    			if(!(id==x+1)){
        			int realRating = rating%10;
        			int sum = 0;
        			int instances = 0;
        			for(int y=0; y<numOfCusts; y++)
        				if(ratings[x][y]!=0&&ratings[id][y]!=0){
        					sum+=(ratings[x][y]-ratings[id][y]);
        					instances++;
        				}
                    if(instances!=0){
                        sum/=instances;
                        totalInstances+=instances;
                        predictedRatings[x][0]+= instances*(realRating+sum);   
                    }
    			}
    		}
            if(totalInstances!=0){
                predictedRatings[x][0]/=totalInstances;
                predictedRatings[x][1]=x;
            }
    	}
    	
    	Arrays.sort(predictedRatings, new java.util.Comparator<double[]>(){
            public int compare(double[]a, double[]b){
                return Double.compare(a[0], b[0]);
            }
        });
    	ArrayList<Integer> indices = new ArrayList<Integer>();
    	for (int a=0;a<25;a++){
    	    indices.add(a);
    	}
    	Collections.shuffle(indices);
    	System.out.println("Here are some movie recommendations for you: ");
    	Double recommendID = 0.0;
    	for(int x=0; x<5; x++){
    		recommendID = predictedRatings[numOfFilms-indices.get(x)][1];
    		System.out.println(movieTitles[(recommendID.intValue())+1];
    	}
    	
    	
    }
}
