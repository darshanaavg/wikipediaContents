package wikipediaContents.http;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticSearch {

	private static RestHighLevelClient client;

	private static final String HOST = "localhost";
	private static final int PORT_ONE = 9200;
	private static final String SCHEME = "http";

	private static final String INDEX = "wikipedia";

	public RestHighLevelClient makeConnection() {

		if (client == null) {
			client = new RestHighLevelClient(RestClient.builder(new HttpHost(HOST, PORT_ONE, SCHEME)));
		}

		System.out.println("Connection to ES established");

		return client;
	}

	public void closeConnection() throws IOException {

		client.close();
		client = null;
		System.out.println("Connection disabled");

	}

	public WikipediaContent insertWikiContent(WikipediaContent content) {

		Map<String, Object> dataMap = new HashMap<String, Object>();

		dataMap.put("key", content.getKey());
		dataMap.put("title", content.getTitle());
		dataMap.put("content", content.getContent());

		IndexRequest indexRequest = new IndexRequest(INDEX).id(content.getPageId() + "").source(dataMap);

		try {

			IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);

		} catch (ElasticsearchException e) {
			e.getDetailedMessage();
		} catch (java.io.IOException ex) {
			ex.getLocalizedMessage();
		}
		return content;
	}

	public void bulkInsert(List<WikipediaContent> contents) {

		BulkRequest bulkRequest = new BulkRequest();

		for (WikipediaContent content : contents) {

			Map<String, Object> dataMap = new HashMap<String, Object>();

			dataMap.put("key", content.getKey());
			dataMap.put("title", content.getTitle());
			dataMap.put("content", content.getContent());

			IndexRequest indexRequest = new IndexRequest(INDEX).id(content.getPageId() + "").source(dataMap);

			bulkRequest.add(indexRequest);
		}
		bulkRequest.timeout("1m");

		try {
			makeConnection();

			BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);

			for (BulkItemResponse bulkItemResponse : response) {

				IndexResponse indexResponse = bulkItemResponse.getResponse();

				System.out.println(indexResponse);
			}

			closeConnection();

		} catch (IOException exception) {
			exception.printStackTrace();
		}
	}
}
