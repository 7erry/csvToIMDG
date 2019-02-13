package com.hazelcast;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;


@CommandLine.Command(description = "Imports csv files into IMDG Maps, assuming first field is index",
name = "csvToIMDG", mixinStandardHelpOptions = true, version = "checksum 3.0")
public class csvToIMDG implements Callable<Void> {

    @CommandLine.Parameters(index = "0", description = "The File name of source")
    private File file;

    @CommandLine.Option(names = { "-v", "--verbose" }, description = "Be verbose.")
    private boolean verbose = false;

    @CommandLine.Option(names = { "-if", "--indexField" }, description = "The field to use an index")
    private String indexField = "";

    @CommandLine.Option(names = { "-it", "--indexType" }, description = "The java type for the index Field")
    private String indexType = "String";

    @CommandLine.Option(names = { "-hc", "--HazelcastConfig" }, description = "The hazelcast client filename defaults to hazelcast-client.xml")
    private String hc = "hazelcast-client.xml";

    @CommandLine.Option(names = { "-mn", "--MapName" }, description = "Map Name of destination defaults to imported")
    private String mapName = "imported";

    private HazelcastInstance hz;

    public static void main(String[] args) {
        CommandLine.call(new csvToIMDG(), args);

    }

    @Override
    public Void call() throws Exception {
        try {

        XmlClientConfigBuilder builder = new XmlClientConfigBuilder(hc);
        ClientConfig config = builder.build();
        hz = HazelcastClient.newHazelcastClient(config);
        IMap map = hz.getMap(mapName);

        Map<String, Map<String, String>> response = new HashMap<>();
        CsvMapper mapper = new CsvMapper();
        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        Map<String, String> entry;
        MappingIterator<Map<String, String>> iterator = mapper.reader(Map.class)
                .with(schema)
                .readValues(file);

        iterator
                .readAll()
                .stream()
                .parallel()
                .forEach((k)->{
                    if(verbose) {
                        System.out.println(k.get(indexField));
                        System.out.println(k);
                    }
                    
                    map.putAsync(k.get(indexField),k);

        });

        if(verbose)
            System.out.println("Imported:\t"+map.size());

        }catch (Exception e){

        }finally {
            hz.shutdown();
        }

        return null;
    }

}