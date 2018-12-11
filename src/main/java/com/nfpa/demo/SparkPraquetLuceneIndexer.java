package com.nfpa.demo;

/**
 * Created by AkshayThali on 23/10/18.
 */

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 * Created by AkshayThali on 09/10/18.
 */
public class SparkPraquetLuceneIndexer implements Serializable {
    //    private static final String SPARK_MASTER = "local[1]";     // "spark://ClairvoyantAdmin:7077";
    private static final String APP_NAME = "SparkReadParquet";
    public static SQLContext sqlContext = null;
    public static SparkConf sparkConf;
    public static JavaSparkContext jsc;
    public static Query phraseQuery;
    public static Query clauseQuery;
    public static BooleanQuery execQuery;
    public static final int TOP_RECORD_LIMIT = 10;
    public static final String EMPTY_FIELD = "EMPTY_FIELD_NA";
    private static boolean createNewIndex = true;
    // public static final String INDEX_DIRECTORY = "file:///tmp/output_indexes_27nov_aks/";.
    // public static final String INDEX_DIRECTORY = "/tmp/tiger_output_luceneindex/";
    public static final String INDEX_DIRECTORY = "/tmp/tiger_output_luceneindex_test28nov/";
    public static final String SCHEMA = "STATEFP,COUNTYFP,FROMHN,TOHN,FEATFULLNAME,NAME,EDGESFULLNAME,ZIPR,lat,lon";
    public static int lineIndex = 0;


    IndexWriterConfig config;
    IndexWriter indexWriter;

    public static List<String> headers = new ArrayList<String>();

    static Logger logger = Logger.getLogger(SparkPraquetLuceneIndexer.class);


    private void disableLogging() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }

    private void doInit() {
        logger.info("Initialize Spark Confs !!!!!!!! ");
        //      sparkConf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APP_NAME);  //local
        sparkConf = new SparkConf().setAppName(APP_NAME);
        jsc = new JavaSparkContext(sparkConf);
        sqlContext = new SQLContext(jsc);
        //System.out.println(" Initialization Done !!!!!!!! ");
        logger.info(" Spark Initialization Done !!!!!!!! ");
        logger.info(" App Name :   " + jsc.appName());
        //System.out.println(" App Name :   " + jsc.appName());
    }


    public void readHDFSParquet() throws Exception {
        logger.info(" readHDFSParquet() ......  invoked  ");

        //String hdfsURI = "/home/DistributedGeocoding/TIGER2018/staged";
        String hdfsURI = "file:///tmp/new_tiger_ip_27nov/";
        // DataFrame parquet = sqlContext.read().parquet("C:/files/myfile.csv.parquet");
        DataFrame tigerAddrDf = sqlContext.read().parquet("file:///tmp/new_tiger_ip_27nov/*.parquet").repartition(10);
        //DataFrame tigerAddrDf = sqlContext.read().parquet("file:///tmp/test_ip_tiger28/*.parquet").repartition(10);
        // Load Parquet DF
        //  DataFrame tigerAddrDf = sqlContext.read().format("parquet").load(hdfsURI);
        // tigerAddrDf.show();


        JavaRDD<Row> rows = tigerAddrDf.toJavaRDD();

//        JavaRDD input =
        rows.map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                String valRow = row.get(0) + "," + row.get(1) + "," + row.get(2) + "," +
                        row.get(3) + "," + row.get(4) + "," + row.get(5) + "," + row.get(6) + "," +
                        row.get(7) + "," + row.get(8) + "," + row.get(9);
                return valRow;
            }
        }).saveAsTextFile("file:///tmp/tiger_output_parquet_to_csv_test28nov/");

//
        // input.collect().forEach(System.out::println);

//        input.foreach(new VoidFunction<String>() {
//            public void call(String line) throws IOException {
//                try {
//                    //   System.out.println("Line to be indexed :" + line);
//                    lineIndex++;
//                    Document d = new Document();
//                    String[] nextLine = line.split(",");
//                    config = getIndexWriterConfig();
//                    indexWriter = new IndexWriter(FSDirectory.open(Paths.get(INDEX_DIRECTORY)), config);
//                    for (String s : SCHEMA.split(","))
//                        headers.add(s);
//                    for (int i = 0; i < nextLine.length; i++) {
//                        String s = nextLine[i];
//                        d.add(new TextField(headers.get(i).trim(), s, Field.Store.YES));
//                        // System.out.println("Header columns : " + headers.get(i).trim() + "\t Columns value : " + s);
//                        //System.out.println("Document to be written " + d.toString());
//                    }
//                    indexWriter.addDocument(d);
//                    if (lineIndex % 10000 == 0) {
//                        System.out.println("Indexed " + lineIndex + " rows.");
//                    }
//                } catch (Exception e) {
//                    System.out.println(e.getMessage());
//                } finally {
//                    indexWriter.close();
//                }
//            }
//        });
        logger.info(" End of the Process()!!!!!!!!!");

    }

    public IndexWriterConfig getIndexWriterConfig() {
        //analyzer with the default stop words
        Analyzer analyzer = new StandardAnalyzer();

        //IndexWriter Configuration
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        if (createNewIndex)
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        else
            config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        return config;
    }


//    public void searchIndexData() throws IOException, org.apache.lucene.queryparser.classic.ParseException {
//        //Read lucene index from the stored directory
//        Directory dir = FSDirectory.open(Paths.get(INDEX_DIRECTORY));  // Test location : /Users/Project Work/NFPA/NFPADemo/ref_addr_index_dir
//
//        //Index reader - an interface for accessing a point-in-time view of a lucene index
//        IndexReader reader = DirectoryReader.open(dir);
//
//        //Create lucene searcher. It search over a single IndexReader.
//        IndexSearcher searcher = new IndexSearcher(reader);
//
//        Analyzer analyzer = new StandardAnalyzer();
//        QueryParser qp = new QueryParser("contents", analyzer);
//
//        //Create the query
//        Query query = qp.parse("EDGESFULLNAME:Browning St");
//        TopDocs topDocs = searcher.search(query, 10);
//
//        ScoreDoc[] hits = topDocs.scoreDocs;
//        for (int i = 0; i < hits.length; i++) {
//            int docId = hits[i].doc;
//            Document d = searcher.doc(docId);
//            System.out.println(d.get("NAME"));
//            // System.out.println(d.toString());
//            //output
//            // fromhn,tohn,addrstate,featfullname,name,edgefullname,countyfp,zipr,lat,lon
//
//            //STATEFP,COUNTYFP,FROMHN,TOHN,FEATFULLNAME,NAME,EDGESFULLNAME,ZIPR,lat,lon
//            System.out.println(d.get("FROMHN") + " | " + d.get("TOHN") + " | " + d.get("STATEFP") + " | " + d.get("FEATFULLNAME") + " | " + d.get("NAME") + " | "
//                    + d.get("EDGESFULLNAME") + " | " + d.get("COUNTYFP") + " | " + d.get("ZIPR") + " | " + d.get("lat") + " | " + d.get("lon")
//            );
//        }
//
//        System.out.println("Found " + hits.length);
//    }

    public static void main(String[] params) throws ParseException, Exception {
        long lStartTime = System.currentTimeMillis(); // start time
        logger.info("------------Started---------------" + new Date().toString());
        SparkPraquetLuceneIndexer loadTable = new SparkPraquetLuceneIndexer();
        loadTable.disableLogging();
        loadTable.doInit();
        loadTable.readHDFSParquet();
        //loadTable.searchIndexData();
        long lEndTime = System.currentTimeMillis(); // end time
        logger.info("------------Ended---------------" + new Date().toString());

        logger.info("Execution time is " + (lEndTime - lStartTime) / 1000d + " seconds");

    }
}
