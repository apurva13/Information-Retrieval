import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.url.WebURL;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.Iterator;
import java.util.HashSet;
import java.util.List;
import java.util.Collections;
import java.util.Set;


public class MyCrawler extends WebCrawler {
	
	private final static Pattern MATCH = Pattern.compile(".*(\\.(html|pdf|gif|jpeg|png))$");

	
    private final static Pattern FILTERS = Pattern.compile(
            ".*(\\.(" + "css|js|json|webmanifest|ttf|svg|wav|avi|mov|mpeg|mpg|ram|m4v|wma|wmv|mid|txt|mp2|mp3|mp4|zip|rar|gz|exe|ico))$");

    private String job1 = "", job2="", job3="" ;

    private HashSet<String> view = new HashSet<String>();

    @Override
    public Object getMyLocalData() {
        return new String[]
        		{job1,
        		job2, 
        		job3};
    }

    // Creating the URLs that will be used by crawler to fetch

    @Override
    protected void handlePageStatusCode(WebURL pageUrl, int statusCode, String statusDescription) {
        String link = pageUrl.getURL().replaceAll(",", "_").toLowerCase();
        job1 = job1 + link + "," + statusCode + "\n";
        view.add(link);
    }

    
     // Two parameters are passed in the below function in which we have the new url 
     // and the function is applied to specify whether the new url should be crawled or not
     // For example, the crawler will ignore urls that have extensions as
     // git, css, js,. and only accept those urls that have "http://www.wsj.com/".
     // Currently, we don't need referringPage parameter.
     
    
  //This function will validate if the URL created is within the scope of the website we are using or not
   
    @Override
    public boolean shouldVisit(Page referringPage, WebURL url) {
        String hyperLink = url.getURL().replaceAll(",", "_").toLowerCase();
        boolean isValid = hyperLink.startsWith("https://www.wsj.com/") || hyperLink.startsWith("http://www.wsj.com/");
        if (isValid==true)
            job3 = job3 + hyperLink + ",OK\n";
        else 
        	job3 = job3 + hyperLink + ",N_OK\n";
        boolean hasNotSeen = !view.contains(hyperLink);
        return !FILTERS.matcher(hyperLink).matches() && hasNotSeen && isValid;
    }

    
     // This function is called when a page is fetched and it is executed by the program
    
     // Validate the files that are downlaoded by the crawler
     
     
    @Override
    public void visit(Page webpage) {
        String link = webpage.getWebURL().getURL().toLowerCase().replaceAll(",", "_");
        int outlinksCount = 0;
        int contentLength = webpage.getContentData().length;
        String typeOFContent = webpage.getContentType().split(";")[0];

        boolean isCorrectType = typeOFContent.contains("pdf") | 
        						typeOFContent.contains("doc") |
                                typeOFContent.contains("html")| 
                                typeOFContent.contains("image");
        if (!isCorrectType)
            return;

        if (webpage.getParseData() instanceof HtmlParseData) {
            HtmlParseData htmlParseData = (HtmlParseData) webpage.getParseData();
            Set<WebURL> links = htmlParseData.getOutgoingUrls();
            outlinksCount += links.size();
        }

        job2 = job2 + link + "," + contentLength + "," + outlinksCount + "," + typeOFContent + "\n";
    }
}