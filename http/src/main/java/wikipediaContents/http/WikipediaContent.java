package wikipediaContents.http;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

public class WikipediaContent {

	private long pageId;
	private String key;
	private String title;
	private String content;

	public static final String wikipediaURL = "https://en.wikipedia.org/w/api.php?action=query&format=json";

	private static final String HOST = "localhost";
	private static final int PORT_ONE = 9200;
	private static final String SCHEME = "http";
	private static final String INDEX = "wikipedia";

	private static RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(HOST, PORT_ONE, SCHEME)));
	
	BulkProcessor.Listener listener = new BulkProcessor.Listener() { 
		int count = 0;

		@Override
		public void beforeBulk(long executionId, BulkRequest request) {

			count = count + request.numberOfActions();
			System.out.println("Uploaded " + count + " so far");

		}

		@Override
		public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {

			if (response.hasFailures()) {
				for (BulkItemResponse bulkItemResponse : response) {
					if (bulkItemResponse.isFailed()) {
						System.out.println(bulkItemResponse.getOpType());
						BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
						System.out.println("Error " + failure.toString());
					}
				}
			}

		}

		@Override
		public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

			System.out.println("Errors - " + failure.toString());

		}
	};

	ThreadPool threadPool = new ThreadPool(Settings.builder().put("elasticsearch", "high-level-client").build());
	
	BulkProcessor bulkProcessor =  new BulkProcessor.Builder(client::bulkAsync, listener, threadPool)
            .build();
	
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

	public void getWikipediaContents(String text)
			throws IOException, InterruptedException, ParseException {

		String titleToSearch = text.trim().replaceAll(" ", "_");

//		List<WikipediaContent> wikiContentList = new ArrayList<WikipediaContent>();

		HttpClient client = HttpClient.newHttpClient();

		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create(wikipediaURL + "&list=search&utf8=1&srsearch=" + titleToSearch + "&srlimit=max"))
				.headers("Content-Type", "application/json;charset=UTF-8").GET().build();
		
		
		
		
		ElasticSearch es = new ElasticSearch();		
		
		

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

//					WikipediaContent w = new WikipediaContent();

//					w.setPageId(pageId);
//					w.setKey(text);
//					w.setTitle(title);
//					w.setContent(content);

					Map<String, Object> dataMap = new HashMap<String, Object>();

					dataMap.put("key", text);
					dataMap.put("title",title);
					dataMap.put("content", content);

					IndexRequest indexRequest = new IndexRequest(INDEX).source(dataMap);
					
					 bulkProcessor.add(indexRequest);
					
//					wikiContentList.add(w);

					System.out.println("Fetching the contents of " + title + " from wikipedia ");
				}

			}

		}
//		client.admin().indices().prepareRefresh().get();
		 System.out.println("Waiting to finish");

         boolean terminated = (bulkProcessor).awaitClose(30L, TimeUnit.SECONDS);
         if(!terminated) {
             System.out.println("Some requests have not been processed");
         } else {
        	 System.out.println("Sucess");
         }

//        client.close();

//		return wikiContentList;
	}
}
