package wikipediaContents.http;

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

public class WikipediaContent {

	private long pageId;
	private String key;
	private String title;
	private String content;

	public static final String wikipediaURL = "https://en.wikipedia.org/w/api.php?action=query&format=json";

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public long getPageId() {
		return pageId;
	}

	public void setPageId(long pageId) {
		this.pageId = pageId;
	}

	public List<WikipediaContent> getWikipediaContents(String text)
			throws IOException, InterruptedException, ParseException {

		String titleToSearch = text.trim().replaceAll(" ", "_");

		List<WikipediaContent> wikiContentList = new ArrayList<WikipediaContent>();

		HttpClient client = HttpClient.newHttpClient();

		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create(wikipediaURL + "&list=search&utf8=1&srsearch=" + titleToSearch + "&srlimit=max"))
				.headers("Content-Type", "application/json;charset=UTF-8").GET().build();

		HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
		String json = response.body().toString();

		JSONParser jsonParser = new JSONParser();
		Object object = jsonParser.parse(json);
		JSONObject jsonObject = (JSONObject) object;

		Object queryObj = jsonObject.get("query");
		JSONObject queryJson = (JSONObject) queryObj;

		JSONArray search = (JSONArray) queryJson.get("search");

		Iterator<?> itr = search.iterator();

		while (itr.hasNext()) {

			Object slide = itr.next();
			JSONObject searchObj = (JSONObject) slide;
			String title = (String) searchObj.get("title");

			long pageId = (long) searchObj.get("pageid");

			String url = wikipediaURL + "&prop=revisions&pageids=" + pageId
					+ "&formatversion=2&rvprop=content&rvslots=*".trim();

			request = HttpRequest.newBuilder().uri(URI.create(url))
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

				if (!pagesObj.containsKey("revisions")) {
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

					WikipediaContent w = new WikipediaContent();

					w.setPageId(pageId);
					w.setKey(text);
					w.setTitle(title);
					w.setContent(content);

					wikiContentList.add(w);

					System.out.println("Fetching the contents of " + title + " from wikipedia ");
				}

			}

		}

		return wikiContentList;
	}
}
