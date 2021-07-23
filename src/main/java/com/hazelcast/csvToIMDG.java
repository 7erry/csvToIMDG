package com.hazelcast;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import picocli.CommandLine;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;


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

    @CommandLine.Option(names = { "-ijf", "--indexJoinFields" }, description = "The fields to combine in order to use an index ie. \"name:age\"")
    private String indexJoinFields = "";

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
        String compoundIndexChar=":";
        try {

        XmlClientConfigBuilder builder = new XmlClientConfigBuilder(hc);
        ClientConfig config = builder.build();
        hz = HazelcastClient.newHazelcastClient(config);
        IMap map = hz.getMap(mapName);

    	if(verbose) System.out.println("Using map:\t"+mapName);

        Map<String, Map<String, String>> response = new HashMap<>();
        CsvMapper mapper = new CsvMapper();
        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        Map<String, String> entry;

	    if(verbose) System.out.println("Importing file:\t"+file);

        // parse csv into Map using header field as keys
        MappingIterator<Map<String, String>> iterator = mapper.reader(Map.class)
                .with(schema)
                .readValues(file);

        // assuming the first line contains header fields
        // skip the header fields
        //iterator.next();

        // sample of two fields combined for index
        if(verbose && !indexJoinFields.isEmpty()) {
            System.out.println(indexJoinFields.split(compoundIndexChar)[0]);
            System.out.println(indexJoinFields.split(compoundIndexChar)[1]);
        }

        if(verbose) System.out.println("Iterator:\t"+iterator);

        iterator
                .readAll()
                .stream()
                .parallel()
                .forEach((k)-> {
                    if (verbose) {
                        if (indexJoinFields.isEmpty()) {
                            System.out.println(k.get(indexField));
                        } else {
                            System.out.println(indexJoinFields.split(compoundIndexChar)[0]);
                            System.out.println(indexJoinFields.split(compoundIndexChar)[0]);
                        }
                        System.out.println(k);
                    }
                    if (indexJoinFields.isEmpty()) {
                        map.set(k.get(indexField), k);
                    } else {
                        map.set(k.get(indexJoinFields.split(compoundIndexChar)[0]) + compoundIndexChar + k.get(indexJoinFields.split(compoundIndexChar)[1]), k);
                    }
                });
        
        if(verbose)
            System.out.println("Imported:\t"+map.size());

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            hz.shutdown();
        }

        return null;
    }

}
