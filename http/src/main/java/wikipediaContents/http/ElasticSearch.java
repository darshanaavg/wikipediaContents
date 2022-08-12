package wikipediaContents.http;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticSearch {

	private static RestHighLevelClient client;

	private static final String HOST = "localhost";
	private static final int PORT_ONE = 9200;
	private static final String SCHEME = "http";

	private static final String INDEX = "wikipedia";
	
	BulkProcessor.Listener listener = new BulkProcessor.Listener() {
        int count = 0;

        @Override
        public void beforeBulk(long l, BulkRequest bulkRequest) {
            count = count + bulkRequest.numberOfActions();
            System.out.println("Uploaded " + count + " so far");
        }

        @Override
        public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
            if (bulkResponse.hasFailures()) {
                for (BulkItemResponse bulkItemResponse : bulkResponse) {
                    if (bulkItemResponse.isFailed()) {
                        System.out.println(bulkItemResponse.getOpType());
                        BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                        System.out.println("Error " + failure.toString());
                    }
                }
            }
        }

        @Override
        public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
            System.out.println("Big errors " + throwable.toString());
        }
    };

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

			IndexResponse response = client.index(indexRequest, null);

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

			BulkResponse response = client.bulk(bulkRequest, null);

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
