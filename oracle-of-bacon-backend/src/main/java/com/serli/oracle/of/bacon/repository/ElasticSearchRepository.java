package com.serli.oracle.of.bacon.repository;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Suggest;
import io.searchbox.core.SuggestResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ElasticSearchRepository {

    private final JestClient jestClient;

    public ElasticSearchRepository() {
        jestClient = createClient();

    }

    public static JestClient createClient() {
        JestClient jestClient;
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
                .multiThreaded(true)
                .readTimeout(60000)
                .build());

        jestClient = factory.getObject();
        return jestClient;
    }

    public List<String> getActorsSuggests(String searchQuery) {
        // TODO implent suggest
        Suggest suggest = new Suggest.Builder("{\n"
                +"\"suggestion\": {\n"
                +"\"text\": \"" + searchQuery + "\",\n"
                +"\"completion\": {\n"
                +"\"field\": \"suggest\"\n"
                +"}\n"
                +"}\n"
                +"}").build();
        List<String> finalResult = new ArrayList<>();
        try {
            SuggestResult result = jestClient.execute(suggest);
            System.out.println(result.getErrorMessage());
            List<SuggestResult.Suggestion> suggestions = result.getSuggestions("suggestion");
            if (suggestions.isEmpty()) {
                return Collections.emptyList();
            }
            return suggestions.get(0).options.stream()
                    .map(option -> ((Map<String, String>)option.get("_source")).get("name"))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }


}
