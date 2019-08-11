package execution;

import java.util.Scanner;

public class QueryParser {
	
	private Scanner console;
	private int numQueries;
	private Query[] queries; 
	
	public QueryParser(Scanner console) {
		this.console = console;
	}
	
	/**
	 * this method parses all the quereies from stdin and builds query object
	 */
	public void parse() {
		numQueries = console.nextInt();
		console.nextLine();
		String[][] queryStrings = new String[numQueries][4];
		for (int i = 0; i < numQueries; i++) {
			for (int j = 0; j < 4; j++) {
				queryStrings[i][j] = console.nextLine();
			}
			if (i < numQueries - 1) console.nextLine();
		}
		console.close();
		queries = new Query[numQueries];
		for (int i = 0; i < numQueries; i++) {
			queries[i] = new Query(queryStrings[i]);
		}
		
	}
	
	public Query[] getQueries() {
		return queries;
	}
	
	public static void main(String[] args) {
		Scanner console = new Scanner(System.in);
		QueryParser qp = new QueryParser(console);
		qp.parse();
	}
	
	
}
