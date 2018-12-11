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
public class TestLuceneQuery implements Serializable {
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
    // public static final String INDEX_DIRECTORY = "file:///tmp/output_indexes_27nov_aks/";
    // public static final String INDEX_DIRECTORY = "/tmp/output_indexes_27nov_aks/";
    public static final String INDEX_DIRECTORY = "/tmp/tiger_new_ip_28nov/";
    public static final String SCHEMA = "STATEFP,COUNTYFP,FROMHN,TOHN,FEATFULLNAME,NAME,EDGESFULLNAME,ZIPR,lat,lon";


    IndexWriterConfig config;
    IndexWriter indexWriter;

    static Logger logger = Logger.getLogger(TestLuceneQuery.class);

    public void searchIndexData(String streetNm,
                                String zipr,
                                String bldNo) throws IOException, org.apache.lucene.queryparser.classic.ParseException {
        //Read lucene index from the stored directory
        Directory dir = FSDirectory.open(Paths.get(INDEX_DIRECTORY));  // Test location : /Users/Project Work/NFPA/NFPADemo/ref_addr_index_dir

        //Index reader - an interface for accessing a point-in-time view of a lucene index
        IndexReader reader = DirectoryReader.open(dir);

        //Create lucene searcher. It search over a single IndexReader.
        IndexSearcher searcher = new IndexSearcher(reader);

        Analyzer analyzer = new StandardAnalyzer();
        QueryParser qp = new QueryParser("contents", analyzer);

        String zipCd = (! zipr.equals(EMPTY_FIELD) && (! StringUtils.isEmpty(zipr) && ! StringUtils.isBlank(zipr)) ? zipr : "NA");
        String numMile = (! bldNo.equals(EMPTY_FIELD) && (! StringUtils.isEmpty(bldNo) && ! StringUtils.isBlank(bldNo)) ? bldNo.replaceAll("[^\\d]", "") : "NA");
        String streetAddr = (! streetNm.equals(EMPTY_FIELD) && (! StringUtils.isEmpty(streetNm) && ! StringUtils.isBlank(streetNm)) ? streetNm : "NA");
        System.out.println("Entered Address     Street Address   :  " + streetAddr + " zip : " + zipCd + " numMile  : " + numMile);

        System.out.println("--- Phrase query to match exact address input [top 10 results]  -----------");

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
                // clauseQuery = qp.parse(" city : " + cityNm + " AND " + " state : " + stateNm);
            }
            execQuery = new BooleanQuery.Builder()
                    .add(new BooleanClause(phraseQuery, BooleanClause.Occur.MUST))
                    .add(new BooleanClause(clauseQuery, BooleanClause.Occur.SHOULD))   //MUST
                    .build();
        }

        long buildQueryEndTime = System.currentTimeMillis(); // end time

        //Create the query
//        phraseQuery = qp.createPhraseQuery("EDGESFULLNAME", "s ridgeview rd");
//        clauseQuery = qp.parse(" ZIPR : " + "66061" + " OR " + "(FROMHN:[ * TO " + "786" + " ] AND TOHN:[ " + "786" + " TO *])");
//        execQuery = new BooleanQuery.Builder()
//                .add(new BooleanClause(phraseQuery, BooleanClause.Occur.MUST))
//                .add(new BooleanClause(clauseQuery, BooleanClause.Occur.SHOULD))   //MUST
//                .build();
        Query query = execQuery;
        System.out.println("\n------------ Query to be executed  ---------------  \n" + query.toString());
        execLuceneQuery(searcher, query);


        System.out.println("\n------ Simple query to match  address input [top 10 results] -------");
        //Create the query
        // Query simpleQuery = qp.parse("EDGESFULLNAME:s ridgeview rd");
        Query simpleQuery = qp.parse("EDGESFULLNAME:" + "'" + streetAddr + "'" + " OR " + " ZIPR:" + zipCd);
        System.out.println("\n------------ Query to be executed  --------------- \n" + simpleQuery.toString());

        execLuceneQuery(searcher, simpleQuery);

    }

    public void execLuceneQuery(IndexSearcher searcher, Query query) throws IOException, org.apache.lucene.queryparser.classic.ParseException {
        long execQueryStartTime = System.currentTimeMillis(); // start time
        //Search for top 10 results
        TopDocs topDocs = searcher.search(query, TOP_RECORD_LIMIT);
        long execQueryEndTime = System.currentTimeMillis(); // end time
        System.out.println("\nTime taken by lucene query to execute and return results  [ ===  Execution time for Lucene query  ===  ] " + (execQueryEndTime - execQueryStartTime) / 1000d + " seconds");

        System.out.println("\nMax Score  : " + searcher.search(query, TOP_RECORD_LIMIT).getMaxScore() + "\t Total hits : " + searcher.search(query, TOP_RECORD_LIMIT).totalHits);

        System.out.println(" FROMHN " + " | " + " TOHN " + " | " + " STATEFP " + " | " + " FEATFULLNAME " + " | " + " NAME " + " | "
                + " EDGESFULLNAME " + " | " + " COUNTYFP " + " | " + " ZIPR " + " | " + " lat " + " | " + " lon " + " | " + " Score "
        );
        ScoreDoc[] hits = topDocs.scoreDocs;
        for (int i = 0; i < hits.length; i++) {
            int docId = hits[i].doc;
            Document d = searcher.doc(docId);

            System.out.println(d.get("FROMHN") + " | " + d.get("TOHN") + " | " + d.get("STATEFP") + " | " + d.get("FEATFULLNAME") + " | " + d.get("NAME") + " | "
                    + d.get("EDGESFULLNAME") + " | " + d.get("COUNTYFP") + " | " + d.get("ZIPR") + " | " + d.get("lat") + " | " + d.get("lon") + " | " + " \tScore :" + hits[i].score
            );
        }
    }

    public static void main(String[] params) throws ParseException, Exception {
        String streetNm = params[0].replaceAll("^\"|\"$", "").trim();
        String zipr = params[1];
        String bldNo = params[2];

        long lStartTime = System.currentTimeMillis(); // start time
        System.out.println("---------  Started  ------------  : \t " + new Date().toString());
        TestLuceneQuery loadTable = new TestLuceneQuery();
        loadTable.searchIndexData(streetNm, zipr, bldNo);
        long lEndTime = System.currentTimeMillis(); // end time
        System.out.println("---------  Ended  -------------  : \t " + new Date().toString());

        System.out.println("Execution time is " + (lEndTime - lStartTime) / 1000d + " seconds");

    }
}
