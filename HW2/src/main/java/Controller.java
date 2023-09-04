import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;



public class Controller {
	
    public static void main(String[] args) throws Exception {
    	
        String storageCrawlData = "../data";// Storage location where the crawled data will be stored
        
        int totalCrawlers = 10;// Defining number of crawlers to run
        int maxCrawlDepth = 16;
        int maxPagesFetched = 20000;
        int politenessDelay = 1000;
        boolean binaryContent = true;
        
        CrawlConfig config = new CrawlConfig();

// Configuration to setup the crawler
        config.setCrawlStorageFolder(storageCrawlData);
        config.setIncludeBinaryContentInCrawling(binaryContent);// to include binary content(image,audio,video) in crawling 
        config.setMaxPagesToFetch(maxPagesFetched);
        config.setPolitenessDelay(politenessDelay);// Defining delay between requests
        config.setMaxDepthOfCrawling(maxCrawlDepth);
//        config.setUserAgentString("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36");

        

        PageFetcher webFetcher = new PageFetcher(config);
        RobotstxtConfig configRobotstxt = new RobotstxtConfig();
        RobotstxtServer serverRobotstxt = new RobotstxtServer(configRobotstxt, webFetcher);
        CrawlController crawlController = new CrawlController(config, webFetcher, serverRobotstxt);

        
        //  Adding the seed URL to start the crawl.

        crawlController.addSeed("https://www.wsj.com");
     
        crawlController.start(MyCrawler.class, totalCrawlers);

        StringBuilder res1 = new StringBuilder("URL,Status\n");
        StringBuilder res2 = new StringBuilder("URL,Size,Outgoing Links,Content Type\n");
        StringBuilder res3 = new StringBuilder("URL,Status\n");

        for (Object t : crawlController.getCrawlersLocalData()) {
            String[] jobs = (String[]) t;

            res1.append(jobs[0]);
            res2.append(jobs[1]);
            res3.append(jobs[2]);
        }

        writeCSV(res1, "fetch_wsj.csv");
        writeCSV(res2, "visit_wsj.csv");
        writeCSV(res3, "urls_wsj.csv");
    }

    private static void writeCSV(StringBuilder output, String s) throws IOException {
        PrintWriter writer = new PrintWriter(s, StandardCharsets.UTF_8);
        writer.println(output.toString().trim());
        writer.flush();
        writer.close();
    }
}