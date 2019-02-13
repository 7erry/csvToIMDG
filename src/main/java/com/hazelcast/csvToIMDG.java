package com.hazelcast;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
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

    public static void main(String[] args) {
        CommandLine.call(new csvToIMDG(), args);

    }

    @Override
    public Void call() throws Exception {
        Map<String, Map<String, String>> response = new HashMap<>();
        CsvMapper mapper = new CsvMapper();
        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        Map<String, String> entry;
        MappingIterator<Map<String, String>> iterator = mapper.reader(Map.class)
                .with(schema)
                .readValues(file);
        while (iterator.hasNext()) {
            entry = iterator.next();

            response.put(entry.get(this.indexField),entry);

        }

        response.forEach((k,v) -> {
            System.out.println(k+": "+v);
            System.out.println("\n");
        });

        return null;
    }

}