package com.nfpa.demo;

/**
 * Created by AkshayThali on 23/10/18.
 */

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.*;


/**
 * Created by AkshayThali on 09/10/18.
 */
public class SparkLuceneGeocoder implements Serializable {
    //    private static final String SPARK_MASTER = "local[1]";     // "spark://ClairvoyantAdmin:7077";
    private static final String APP_NAME = "SparkGeocodeDemo";
    public static SQLContext sqlContext = null;
    public static SparkConf sparkConf;
    public static JavaSparkContext jsc;
    public static Query phraseQuery;
    public static Query clauseQuery;
    public static BooleanQuery execQuery;
    public static final String EMPTY_FIELD = "EMPTY_FIELD_NA";
    public static Query query;

    static Logger logger = Logger.getLogger(SparkLuceneGeocoder.class);

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


    public void csvLuceneIndexer(String HDFS_OUTPUT_DIR, String LUCENE_INDEXES_STORE_DIR,
                                 String IP_BUSINESS_ADDRESS, int TOP_RECORD_LIMIT, int NO_OF_PARTITIONS) throws Exception {
        logger.info(" csvLuceneIndexer() ......  invoked  ");

        // Load CSV DF
        DataFrame nfpaAddrDf = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(IP_BUSINESS_ADDRESS);

        //nfpaAddrDf.show();
        // JavaRDD<Row> addressRDD = nfpaAddrDf.select("streetname", "state", "city", "zip", "street_pre", "num_mile", "streettype").limit(10).repartition(10).toJavaRDD();
        // JavaRDD<Row> addressRDD = nfpaAddrDf.select("streetname", "state", "city", "zip5", "street_pre", "num_mile", "streettype").repartition(10).toJavaRDD();

        JavaRDD<Row> addressRDD = nfpaAddrDf.select("streetname", "state", "city", "zip5", "street_pre", "num_mile", "streettype").repartition(NO_OF_PARTITIONS).toJavaRDD();
        //JavaRDD<Row> addressRDD = nfpaAddrDf.select("streetname", "state", "city", "zip5", "street_pre", "num_mile", "streettype").limit(10).toJavaRDD();

        JavaRDD flattenAddr = addressRDD.map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                String flattenAddrStr = ((! StringUtils.isEmpty(row.get(0).toString()) && ! StringUtils.isBlank(row.get(0).toString())) ? row.get(0) : EMPTY_FIELD) + "," +
                        ((! StringUtils.isEmpty(row.get(1).toString()) && ! StringUtils.isBlank(row.get(1).toString())) ? row.get(1) : EMPTY_FIELD) + "," +
                        ((! StringUtils.isEmpty(row.get(2).toString()) && ! StringUtils.isBlank(row.get(2).toString())) ? row.get(2) : EMPTY_FIELD) + "," +
                        ((! StringUtils.isEmpty(row.get(3).toString()) && ! StringUtils.isBlank(row.get(3).toString())) ? row.get(3) : EMPTY_FIELD) + "," +
                        ((! StringUtils.isEmpty(row.get(4).toString()) && ! StringUtils.isBlank(row.get(4).toString())) ? row.get(4) : EMPTY_FIELD) + "," +
                        ((! StringUtils.isEmpty(row.get(5).toString()) && ! StringUtils.isBlank(row.get(5).toString())) ? row.get(5) : EMPTY_FIELD) + "," +
                        ((! StringUtils.isEmpty(row.get(6).toString()) && ! StringUtils.isBlank(row.get(6).toString())) ? row.get(6) : EMPTY_FIELD);
                return flattenAddrStr;
            }
        });

        JavaRDD<Map<String, List<String>>> resultRDD = flattenAddr.map(new Function<String, Map<String, List<String>>>() {
            @Override
            public Map<String, List<String>> call(String searchColumns) throws Exception {
                logger.info("Value in RDD : - " + searchColumns);
                //if (!s.isEmpty() && s.length() > 1) {
                Map<String, List<String>> result = searchLuceneIndex(searchColumns, LUCENE_INDEXES_STORE_DIR, TOP_RECORD_LIMIT);
                return result;
            }
        });


        logger.info(" Save the Results : ");
//test 1       // resultRDD.saveAsTextFile("/Users/Project_Work/NFPA/SparkGeocodeDemo/result_output_test");
        resultRDD.saveAsTextFile(HDFS_OUTPUT_DIR);
        //System.out.println("End of the Process()!!!!!!!!!");
        logger.info(" End of the Process()!!!!!!!!!");

    }

    public Map<String, List<String>> searchLuceneIndex(String searchColumns, String LUCENE_INDEXES_STORE_DIR, int TOP_RECORD_LIMIT) throws IOException, org.apache.lucene.queryparser.classic.ParseException {

        //Read from Directory where the indexes are stored
        Directory dir = FSDirectory.open(Paths.get(LUCENE_INDEXES_STORE_DIR));
        // Index reader - an interface for accessing a point-in-time view of a lucene index
        IndexReader reader = DirectoryReader.open(dir);
        //Create lucene searcher. It search over a single IndexReader.
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer = new StandardAnalyzer();
        // analyzer = new StandardAnalyzer();
        QueryParser qp = new QueryParser("contents", analyzer);
        //qp = new QueryParser("contents", analyzer);

        try {
            logger.info("Inside the Process  searchLuceneIndex()!!!!!!!!!");
            //System.out.println("Look up for address :  " + searchColumns);

            //Get the respective columns :
            String[] arrSearchFields = searchColumns.split(",");
            //  "streetname", "state", "city", "zip", "street_pre", "num_mile","streettype"
            String streetName = (! arrSearchFields[0].equals(EMPTY_FIELD) ? arrSearchFields[0] : "NA");
            String stateNm = (! arrSearchFields[1].equals(EMPTY_FIELD) ? arrSearchFields[1] : "NA");
            String cityNm = (! arrSearchFields[2].equals(EMPTY_FIELD) ? arrSearchFields[2] : "NA");
            String zipCd = (! arrSearchFields[3].equals(EMPTY_FIELD) ? arrSearchFields[3] : "NA");
            String streetPrefix = (! arrSearchFields[4].equals(EMPTY_FIELD) ? arrSearchFields[4].trim() : "");
            String numMile = (! arrSearchFields[5].equals(EMPTY_FIELD) ? arrSearchFields[5].replaceAll("[^\\d]", "") : "NA");
            String streetType = (! arrSearchFields[6].equals(EMPTY_FIELD) ? arrSearchFields[6].trim() : "");
            String streetAddr = (streetPrefix + " " + streetName + " " + streetType).trim();

            //System.out.println(" street   :  " + street + " state : " + state + " city : " + city + " zip : " + zip + " streetname : " + streetname + " numMile  : " + numMile);
            logger.info(" Street Address   :  " + streetAddr + " state : " + stateNm + " city : " + cityNm + " zip : " + zipCd + " numMile  : " + numMile);

            long buildQueryStartTime = System.currentTimeMillis(); // start time

            if ((! streetAddr.isEmpty() && (streetAddr != null) && ! streetAddr.equals("NA")) && (! zipCd.isEmpty() && (zipCd != null) && ! zipCd.equals("NA") && ! zipCd.equals("0"))) {
                //TODO  add the clause for building no./ city / state
                phraseQuery = qp.createPhraseQuery("EDGESFULLNAME", streetAddr);
                if (! numMile.isEmpty() && numMile != null && ! numMile.equals("NA")) {
                    clauseQuery = qp.parse(" ZIPR : " + zipCd + " OR " + "(FROMHN:[ * TO " + numMile + " ] AND TOHN:[ " + numMile + " TO *])");
                    //  + " AND " + " city : " + city + " AND " + " state : " + state);   // TODO Scores are affected when this clause used
                } else {
                    clauseQuery = qp.parse(" ZIPR : " + zipCd);
                    // + " AND " + " city : " + cityNm + " AND " + " state : " + stateNm);  // TODO Scores are affected when this clause used
                }
                execQuery = new BooleanQuery.Builder()
                        .add(new BooleanClause(phraseQuery, BooleanClause.Occur.MUST))
                        .add(new BooleanClause(clauseQuery, BooleanClause.Occur.SHOULD))   //MUST
                        .build();

            } else if ((streetAddr.isEmpty() && streetAddr == null && streetAddr.equals("NA")) || (! zipCd.isEmpty() && zipCd != null && ! zipCd.equals("NA") && ! zipCd.equals("0"))
                //TODO  || ((!city.isEmpty() && city != null)) || (!state.isEmpty() && state != null)
                    ) {
                if (! numMile.isEmpty() && numMile != null && ! numMile.equals("NA")) {
                    clauseQuery = qp.parse(" ZIPR : " + zipCd + " OR " + "(FROMHN:[ * TO " + numMile + " ] AND TOHN:[ " + numMile + " TO *])");
                    // + " AND " + " city : " + city + " AND " + " state : " + state);   // TODO Scores are affected when this clause used
                } else {
                    clauseQuery = qp.parse(" ZIPR : " + zipCd); // TODO Scores are affected when this clause used
                    //+ " AND " + " city : " + cityNm + " AND " + " state : " + stateNm);
                }
                execQuery = new BooleanQuery.Builder()
                        .add(new BooleanClause(clauseQuery, BooleanClause.Occur.SHOULD))   //MUST
                        .build();

            } else if ((zipCd.isEmpty() && zipCd == null && zipCd.equals("NA") && zipCd.equals("0")) || (! streetAddr.isEmpty() && streetAddr != null && ! streetAddr.equals("NA"))
                //TODO   || ((!city.isEmpty() && city != null)) || (!state.isEmpty() && state != null)
                    ) {
                phraseQuery = qp.createPhraseQuery("EDGESFULLNAME", streetAddr);
                if (! numMile.isEmpty() && numMile != null && ! numMile.equals("NA")) {
                    clauseQuery = qp.parse("(FROMHN:[ * TO " + numMile + " ] AND TOHN:[ " + numMile + " TO *])");
                    // + " AND " + " city : " + city + " AND " + " state : " + state);    // TODO Scores are affected when this clause used
                } else {
                    clauseQuery = qp.parse(" city : " + cityNm + " AND " + " state : " + stateNm);
                }
                execQuery = new BooleanQuery.Builder()
                        .add(new BooleanClause(phraseQuery, BooleanClause.Occur.MUST))
                        .add(new BooleanClause(clauseQuery, BooleanClause.Occur.SHOULD))   //MUST
                        .build();
            }

            long buildQueryEndTime = System.currentTimeMillis(); // end time
            logger.info("Execution time taken to build the query [ === Build query with clauses ===  ] " + (buildQueryEndTime - buildQueryStartTime) / 1000d + " seconds");

            query = execQuery;
            displayQuery(query);

            long execQueryStartTime = System.currentTimeMillis(); // start time
            //Search for top 10 results
            TopDocs topDocs = searcher.search(query, TOP_RECORD_LIMIT);
            long execQueryEndTime = System.currentTimeMillis(); // end time
            logger.info("Time taken by lucene query to execute  [ ===  Execution time for Lucene query  ===  ] " + (execQueryEndTime - execQueryStartTime) / 1000d + " seconds");

            //System.out.println(" Max Score  : " + searcher.search(query, TOP_RECORD_LIMIT).getMaxScore() + "\t Total hits : " + searcher.search(query, TOP_RECORD_LIMIT).totalHits);
            //logger.info(" Max Score  : " + searcher.search(query, TOP_RECORD_LIMIT).getMaxScore() + "\t Total hits : " + searcher.search(query, TOP_RECORD_LIMIT).totalHits);
            //String result = "";
            List resultLst = new ArrayList();
            Map<String, List<String>> resultMap = new HashMap<>();
            ScoreDoc[] hits = topDocs.scoreDocs;

            for (int i = 0; i < hits.length; i++) {
                int docId = hits[i].doc;
                Document d = searcher.doc(docId);

                resultLst.add(d.get("FROMHN") + " | " + d.get("TOHN") + " | " + d.get("STATEFP") + " | " + d.get("FEATFULLNAME") + " | " + d.get("NAME") + " | "
                        + d.get("EDGESFULLNAME") + " | " + d.get("COUNTYFP") + " | " + d.get("ZIPR") + " | " + d.get("lat") + " | " + d.get("lon") + " | " + " \tScore :" + hits[i].score);
            }

            resultMap.put(searchColumns, resultLst);
            return resultMap;
        } finally {
            reader.close();
            analyzer.close();
        }

    }

    public static void displayQuery(Query query) {
        //System.out.println("Query Executed : " + query.toString());
        logger.info("Query Executed : " + query.toString());
    }

    public static void main(String[] params) throws ParseException, Exception {
        String HDFS_OUTPUT_DIR = params[0];
        String LUCENE_INDEXES_STORE_DIR = params[1];
        String IP_BUSINESS_ADDRESS = params[2];
        int TOP_RECORD_LIMIT = Integer.parseInt(params[3]);
        int NO_OF_PARTITIONS = Integer.parseInt(params[4]);
        long lStartTime = System.currentTimeMillis(); // start time
        logger.info("------------Started---------------" + new Date().toString());
        SparkLuceneGeocoder loadTable = new SparkLuceneGeocoder();
        loadTable.disableLogging();
        loadTable.doInit();
        loadTable.csvLuceneIndexer(HDFS_OUTPUT_DIR, LUCENE_INDEXES_STORE_DIR, IP_BUSINESS_ADDRESS, TOP_RECORD_LIMIT, NO_OF_PARTITIONS);
        long lEndTime = System.currentTimeMillis(); // end time
        logger.info("------------Ended---------------" + new Date().toString());

        logger.info("Execution time is " + (lEndTime - lStartTime) / 1000d + " seconds");

    }
}

