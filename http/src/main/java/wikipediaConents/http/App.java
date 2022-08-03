package wikipediaConents.http;

import java.util.Scanner;
import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Iterator;

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
		String titleToSearch = text.trim().replaceAll(" ", "_");

		HttpClient client = HttpClient.newHttpClient();
		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create(wikipediaURL + "&list=search&utf8=1&srsearch=" + titleToSearch + "&srlimit=max"))
				.headers("Content-Type", "application/json;charset=UTF-8")
				.GET()
				.build();

		HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
		String json = response.body().toString();

		JSONParser jsonParser = new JSONParser();
		Object object = jsonParser.parse(json);
		JSONObject jsonObject = (JSONObject) object;

		Object queryObj = jsonObject.get("query");
		JSONObject queryJson = (JSONObject) queryObj;

		JSONArray search = (JSONArray) queryJson.get("search");

		Iterator<?> itr = search.iterator();

		String file = "D:\\wikiContents_" + titleToSearch + "_.txt";
		File newFile = new File(file);
		FileWriter fw = new FileWriter(newFile);

		while (itr.hasNext()) {

			Object slide = itr.next();
			JSONObject searchObj = (JSONObject) slide;
			String title = (String) searchObj.get("title");

			fw.write("\r\nTitle: " + title + "\r\n");

			title = title.trim().replaceAll(" ", "_");
			
			String url = wikipediaURL + "&prop=revisions&titles=" + title + "&formatversion=2&rvprop=content&rvslots=*".trim();

			request = HttpRequest.newBuilder()
					.uri(URI.create(url))
					.headers("Content-Type", "application/json;charset=UTF-8").GET().build();

			response = client.send(request, HttpResponse.BodyHandlers.ofString());

			String contentResponse = response.body();

			Object responseObj = jsonParser.parse(contentResponse);
			JSONObject responseJson = (JSONObject) responseObj;

			Object queryObj1 = responseJson.get("query");
			JSONObject queryJson1 = (JSONObject) queryObj1;

			JSONArray pages = (JSONArray) queryJson1.get("pages");

			Iterator<?> pageItr = pages.iterator();

			while (pageItr.hasNext()) {

				Object next = pageItr.next();
				JSONObject pagesObj = (JSONObject) next;

				if(!pagesObj.containsKey("revisions")) {
					break;
				}

				JSONArray revisions = (JSONArray) pagesObj.get("revisions");

				Iterator<?> revisionItr = revisions.iterator();

				while (revisionItr.hasNext()) {

					Object nextObj = revisionItr.next();
					JSONObject revisionsJson = (JSONObject) nextObj;

					Object slotsObj = revisionsJson.get("slots");
					JSONObject slotsJson = (JSONObject) slotsObj;

					Object mainObj = slotsJson.get("main");
					JSONObject mainJson = (JSONObject) mainObj;

					String content = (String) mainJson.get("content");

					System.out.println("Copying the contents of " + title);
					fw.write(content);
				}

			}

		}
		if (newFile.length() == 0) {
			System.out.println("Contents missing in the wikipedia for the title " + text);
		} else {
			System.out.println("All the wikipedia searches are successfully copied to the file in the location " + file);
			fw.close();
		}

	}

}
