package wikipediaContents.http;

import java.util.Scanner;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class App {

	public static final String wikipediaURL = "https://en.wikipedia.org/w/api.php?action=query&format=json";

	public static void main(String[] args) throws IOException, InterruptedException, ParseException

	{
		Scanner inputObj = new Scanner(System.in);

		System.out.println("Give the title to search in wikipedia\n");

		String text = inputObj.nextLine();

		WikipediaContent w = new WikipediaContent();
		
		w.getWikipediaContents(text);

		System.out.println("All the contents are moved to Elastic Search");

	}

}