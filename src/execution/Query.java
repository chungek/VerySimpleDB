package execution;

import java.util.ArrayList;
import java.util.Arrays;

public class Query {
	
	public String[] sumsNeeded; 		
	String[] relationsNeeded;	
	String[] joinPreds;			
	String[] filterPreds;			
	
	public Query(String[] queryInLine) {
		sumsNeeded		= queryInLine[0].replace("SELECT ", "").split(", ");
		relationsNeeded	= queryInLine[1].replace("FROM ", "").split(", ");
		joinPreds		= queryInLine[2].replace("WHERE ", "").split(" AND ");
		filterPreds		= queryInLine[3].replace(";", "").replaceAll("and", "AND")
										.replaceFirst("AND ", "").split(" AND ");
	}
}
