package execution;

import java.util.ArrayList;
import java.util.Arrays;

public class Query {
	
	public String[] sumsNeeded; 			//[SUM(A.c32), SUM(A.c35), SUM(A.c25)]
	String[] relationsNeeded;		//[A, C, F, J, G, H, D]
	String[] joinPreds;				//[A.c2 = C.c0, A.c5 = F.c0, A.c9 = J.c0, A.c6 = G.c0, A.c7 = H.c0, A.c3 = D.c0]
	String[] filterPreds;			//[A.c46 < -2101, J.c1 > 0, F.c2 = 0]
	
	public Query(String[] queryInLine) {
		sumsNeeded		= queryInLine[0].replace("SELECT ", "").split(", ");
		relationsNeeded	= queryInLine[1].replace("FROM ", "").split(", ");
		joinPreds		= queryInLine[2].replace("WHERE ", "").split(" AND ");
		filterPreds		= queryInLine[3].replace(";", "").replaceAll("and", "AND")
										.replaceFirst("AND ", "").split(" AND ");
	}
}
