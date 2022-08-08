package wikipediaContents.http;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticSearch {

	private static RestHighLevelClient restHighLevelClient;

	private static final String HOST = "localhost";
	private static final int PORT_ONE = 9200;
	private static final String SCHEME = "http";

	private static final String INDEX = "wikipedia";

	public void makeConnection() {

		if (restHighLevelClient == null) {
			restHighLevelClient = new RestHighLevelClient(
					RestClient.builder(new HttpHost(HOST, PORT_ONE, SCHEME)));
		}

	}
	
	public void closeConnection() throws IOException {
	    restHighLevelClient.close();
	    restHighLevelClient = null;
	}
	
	public WikipediaContent insertWikiContent(WikipediaContent content){
		
	    Map<String, Object> dataMap = new HashMap<String, Object>();
	    
	    dataMap.put("key", content.getKey());
	    dataMap.put("title", content.getTitle());
	    dataMap.put("content", content.getContent());
	    
	    IndexRequest indexRequest = new IndexRequest(INDEX);
	    indexRequest.id(content.getPageId()+"");	    
	    indexRequest.source(dataMap);
	    
	    try {
	        IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
	        System.out.println(response.getId());
	    } catch(ElasticsearchException e) {
	        e.getDetailedMessage();
	    } catch (java.io.IOException ex){
	        ex.getLocalizedMessage();
	    }
	    return content;
	}
}
