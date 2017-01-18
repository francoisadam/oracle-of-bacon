package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.mapping.PutMapping;


import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CompletionLoader {
    private static AtomicInteger count = new AtomicInteger(0);

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }

        String inputFilePath = args[0];
        JestClient client = ElasticSearchRepository.createClient();

        client.execute(new CreateIndex.Builder("actors").build());
        PutMapping putMapping = new PutMapping.Builder(
                "actors",
                "actor",
                "{\"actor\" : {\n"
                        +"\"properties\" : {\n"
                        +"\"suggest\" : {\n"
                        +"\"type\" : \"completion\"\n"
                        +"},\n"
                        +"\"name\" : {\n"
                        +"\"type\": \"text\"\n"
                        +"}\n"
                        +"}\n"
                        +"}\n}"
        ).build();
        client.execute(putMapping);

        List<Index> actorIndexes = new ArrayList<>();
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            int bulkSize = 100000;
            bufferedReader.lines().skip(1).forEach(line -> {
                line = line.substring(1, line.length() - 1);
                String[] names = line.split(", ");
                String lastName = names[0];
                String firstName = names.length > 1 ? names[1] : null;
                String actor;

                if (firstName != null) {
                    actor = "{\"name\":\"" + line + "\", \"suggest\": [\"" + firstName + "\", \"" + lastName + "\"]}";
                }
                else {
                    actor = "{\"name\":\"" + line + "\", \"suggest\": [\"" + lastName + "\"]}";
                }

                Index actorIndex = new Index.Builder(actor).index("actors").type("actor").build();
                actorIndexes.add(actorIndex);

                if (actorIndexes.size() == bulkSize)  {
                    Bulk bulk = new Bulk.Builder()
                            .defaultIndex("actors")
                            .defaultType("actor")
                            .addAction(actorIndexes)
                            .build();
                    try {
                        client.execute(bulk);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    actorIndexes.clear();
                }
                count.incrementAndGet();
            });
        }
        // Remaining
        Bulk bulk = new Bulk.Builder()
                .defaultIndex("actors")
                .defaultType("actor")
                .addAction(actorIndexes)
                .build();
        try {
            client.execute(bulk);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Inserted total of " + count.get() + " actors");
    }
}
