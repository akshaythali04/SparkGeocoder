package com.nfpa.demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * Created by AkshayThali on 23/10/18.
 */
public class CreateLuceneIndex {

    private File csvFile;
    private boolean createNewIndex;
    public static final String SCHEMA = "STATEFP,COUNTYFP,FROMHN,TOHN,FEATFULLNAME,NAME,EDGESFULLNAME,ZIPR,lat,lon";
    //Test Dir   :    /Users/Project Work/NFPA/NFPADemo/ref_addr_index_dir
    //public static final String INDEX_DIRECTORY = "/home/clairvoyant/nfpa_test/tiger_ca_ref_v2_indexes";

    public CreateLuceneIndex(File csvFile, boolean createNewIndex) {
        this.csvFile = csvFile;
        this.createNewIndex = createNewIndex;
    }

    public void parseAndCreateIndex(String INDEX_DIRECTORY) throws IOException {
        if (! csvFile.exists()) {
            throw new FileNotFoundException("CSV file not found: " + csvFile);
        }

        // Prepare the index writer of Lucene
        IndexWriterConfig config = getIndexWriterConfig();
        IndexWriter indexWriter = new IndexWriter(FSDirectory.open(Paths.get(INDEX_DIRECTORY)), config);

        // Start reading the CSV file
        CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(csvFile), "UTF-8"));
        String[] nextLine;
        int lineIndex = 0;
        List<String> headers = new ArrayList<String>();
        for (String s : SCHEMA.split(","))
            headers.add(s);

        while ((nextLine = reader.readNext()) != null) {
            lineIndex++;
            // Check for the headers in the first line
//            if (lineIndex == 1) {
//                for (String s : nextLine)
//                    headers.add(s);
//            }

            // Populate the index with data
            //else {
            // Skip the lines where the # values != number of header names
            if (nextLine.length != headers.size())
                continue;
            Document d = new Document();
            for (int i = 0; i < nextLine.length; i++) {

                String s = nextLine[i];
                // Skip empty values
                if (s.trim().equals(""))
                    continue;
                //d.add(new TextField(headers.get(i), s, Field.Store.YES, Field.Index.NOT_ANALYZED));
                d.add(new TextField(headers.get(i), s, Field.Store.YES));
            }
            indexWriter.addDocument(d);
            // }
            if (lineIndex % 10000 == 0) {
                System.out.println("Indexed " + lineIndex + " rows.");
            }
        }
        indexWriter.close();
        reader.close();
        System.out.println("Done creating index.");
    }

    public IndexWriterConfig getIndexWriterConfig() {
        //analyzer with the default stop words
        Analyzer analyzer = new StandardAnalyzer();

        //IndexWriter Configuration
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        if (createNewIndex)
            //  config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        else
            config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        return config;
    }


    /*public void searchIndexData() throws IOException, org.apache.lucene.queryparser.classic.ParseException {
        //Read lucene index from the stored directory
        Directory dir = FSDirectory.open(Paths.get("/home/clairvoyant/nfpa_test/tiger_ca_ref_v2_indexes"));  // Test location : /Users/Project Work/NFPA/NFPADemo/ref_addr_index_dir

        //Index reader - an interface for accessing a point-in-time view of a lucene index
        IndexReader reader = DirectoryReader.open(dir);

        //Create lucene searcher. It search over a single IndexReader.
        IndexSearcher searcher = new IndexSearcher(reader);

        Analyzer analyzer = new StandardAnalyzer();
        QueryParser qp = new QueryParser("contents", analyzer);

        //Create the query
        //Query query = qp.parse("name : Silver Lake");        // Test Condition   :  street_addr:Ap
        //Query query = qp.parse("edgefullname : Silver Lake");
        Query query = qp.parse("addrstate : 06");
        TopDocs topDocs = searcher.search(query, 100);

        ScoreDoc[] hits = topDocs.scoreDocs;
        for (int i = 0; i < hits.length; i++) {
            int docId = hits[i].doc;
            Document d = searcher.doc(docId);
            System.out.println(d.get("name"));
            // System.out.println(d.toString());
            //output
            // fromhn,tohn,addrstate,featfullname,name,edgefullname,countyfp,zipr,lat,lon

            System.out.println(d.get("fromhn") + " | " + d.get("tohn") + " | " + d.get("addrstate") + " | " + d.get("featfullname") + " | " + d.get("name") + " | "
                    + d.get("edgefullname") + " | " + d.get("countyfp") + " | " + d.get("zipr") + " | " + d.get("lat") + " | " + d.get("lon")
            );
        }

        System.out.println("Found " + hits.length);
    }*/

    public static void main(String[] args) throws Exception {
        System.out.println("------------ Started Indexing Process ---------------" + new Date().toString());
        String INDEX_DIRECTORY = args[0];
        String INPUT_CSV = args[1];    // /tmp/tiger_output_parquet_to_csv_test28nov/
        //  Test Dir    /Users/Project Work/NFPA/NFPADemo/src/main/resources/input_ref_addr_data.txt
        //"/home/clairvoyant/nfpa_test/nfpa_sample_tiger_ip_data_ca/tiger_ca_ref_v2.csv"

        //List the csv files and run the index process for each file
        File myDirectory = new File(INPUT_CSV);
        String[] containingFileNames = myDirectory.list();
        for (String fileName : containingFileNames) {
            if (fileName.matches("part-\\d*")) {
                System.out.println("File name :  ");
                System.out.println(fileName);
                CreateLuceneIndex crt = new CreateLuceneIndex(new File(INPUT_CSV + fileName), true);
                try {
                    //Create index
                    crt.parseAndCreateIndex(INDEX_DIRECTORY);
                    System.out.println("Done creating index for file : " + INPUT_CSV + fileName);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

//        CreateLuceneIndex crt = new CreateLuceneIndex(new File(INPUT_CSV), true);
//        try {
//            //Create index
//            crt.parseAndCreateIndex(INDEX_DIRECTORY);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        //Invoke search
        //crt.searchIndexData();

        System.out.println("------------ Ended Indexing Process ---------------" + new Date().toString());

    }

}
