package curd;
/**
 * @author Abhijeet Gulve
 */

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public class CurdOperation {
    public static void main(String[] args) {

        Client trClient = null;
        try {
            trClient = new PreBuiltTransportClient(
                    Settings.builder().put("client.transport.sniff", true)
                            .put("cluster.name", "elasticsearch").build())
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        trClient.prepareIndex("test", "article", "2")
                .setSource(putJsonDocument("ElasticSearch: Java API",
                        "ElasticSearch provides the Java API, all operations "
                                + "can be executed asynchronously using a client object.",
                        new Date(),
                        new String[]{"elasticsearch"},
                        "Hüseyin Akdoğan")).execute().actionGet();

        updateDocument(trClient, "test", "article");
        searchDocument(trClient, "test");
        deleteDocument(trClient, "test", "article", "2");
//        GetResponse getResponse = trClient.prepareGet("test", "article", "2").execute().actionGet();
//        Map<String, Object> source = getResponse.getSource();
//        System.out.println("------------------------------");
//        System.out.println("Index: " + getResponse.getIndex());
//        System.out.println("Type: " + getResponse.getType());
//        System.out.println("Id: " + getResponse.getId());
//        System.out.println("Version: " + getResponse.getVersion());
//        System.out.println(source);
//        System.out.println("------------------------------");
    }

    public static void searchDocument(Client client, String index) {
        SearchResponse response = client.prepareSearch(index)
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();
        SearchHit[] results = response.getHits().getHits();
        System.out.println("Current results: " + results.length);
        for (SearchHit hit : results) {
            System.out.println("------------------------------");
            Map<String, Object> result = hit.getSource();
            System.out.println(result);
        }
    }


    public static void updateDocument(Client client, String index, String type) {

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index);
        updateRequest.type(type);
        updateRequest.id("2");
        try {
            updateRequest.doc(jsonBuilder()
                    .startObject()
                    .field("title", "updated text")
                    .endObject());
            client.update(updateRequest).get();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void deleteDocument(Client client, String index, String type, String id) {
        DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();
        System.out.println("Information on the deleted document:");
        System.out.println("Index: " + response.getIndex());
        System.out.println("Type: " + response.getType());
        System.out.println("Id: " + response.getId());
        System.out.println("Version: " + response.getVersion());
    }

    public static Map<String, Object> putJsonDocument(String title, String content, Date postDate,
                                                      String[] tags, String author) {
        Map<String, Object> jsonDocument = new HashMap<String, Object>();
        jsonDocument.put("title", title);
        jsonDocument.put("conten", content);
        jsonDocument.put("postDate", postDate);
        jsonDocument.put("tags", tags);
        jsonDocument.put("author", author);
        return jsonDocument;
    }
}
