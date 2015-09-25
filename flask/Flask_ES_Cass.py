import json


query_phrase = 'flirtatious'
index_name='test-32_large'
def elasticsearch(query_phrase,index_name):
    #Query Elasticsearch to see what URLs contain this query
    #result = es.search(index="movie_db", body={'query': {'match': {'description': 'CIA'}}})
    from elasticsearch import Elasticsearch
    es = Elasticsearch(['52.88.228.98'])
    #Query Elasticsearch to see what URLs contain this query
    res = es.search(index_name, q=query_phrase)
    #Calculate hits out of the total amount
    Hits=res['hits']['total']
    #Find total amount by query that is a false positive, 100%, wiki search
    total_Hits = (es.search(index_name, q='wiki'))['hits']['total'] #modify later to direct call to get total amount
    #Output
    print( ("Got %d Hits:" % res['hits']['total']) + ' out of total: ' + str(total_Hits))
    return res

#return the elastic search item
res=elasticsearch(query_phrase,index_name)


#Formatting goal to avoid errors
#print session.execute("SELECT ranks FROM url_ranks_3 WHERE url='http://en.wikipedia.org/wiki/Shareef_Cousin';")

#declare empty arrays for filling with information
url_total=[]
ranks_total=[]
links_total=[]
links_listedPerURL=[]
    
def extract_queried_urls_ranks_links(res,url_total,ranks_total,links_total,links_listedPerURL):
    from cqlengine import connection
    from cassandra.cluster import Cluster
    CASSANDRA_KEYSPACE = "test"
    connection.setup(['52.88.228.98','52.11.49.170'],CASSANDRA_KEYSPACE)
    cluster = Cluster(['52.88.228.98','52.11.49.170'])
    session = cluster.connect(CASSANDRA_KEYSPACE)
    #Loop through ElasticSearch then loop through 
    for hit in res['hits']['hits']:
        key_lookup= ("%(url)s"% hit["_source"])
        #modify the url so that cqlsh can read it, even though it has a colon
        key_lookup="'" + key_lookup + "';"
        query1 = "SELECT * FROM url_ranks_4 WHERE url="+key_lookup
        #print query1
        #consult the cassandra table with the above query
        rows=(session.execute(query1))
        #check to make sure we are not taking information from nodes that are not currently in elastic search or cassandra
        for row in rows:
            if row[1] != None: #row[0]=url, row[1]=rank
                
                ###ADD HERE###
                #Declare filtering step by page rank value
                #Restrict to the top 1000 to avoid crashing the EC2 micro instance
                ###ADD HERE###
                
                #print row[0]
                key_lookup_for_links="'" + str(row[0]) + "';"
                #print key_lookup
                query2 = "SELECT * FROM url_links_3 WHERE url="+key_lookup_for_links
                #print query2
                rows_links=(session.execute(query2))
                #have a method to export all information here
                for row_links in rows_links:
                    #print row_links[0], row[1], row_links[1]
                    print row_links[0], row[1], len(row_links[1])
                    url_total.append(row_links[0])
                    ranks_total.append(row[1])
                    links_total.append(row_links[1])
                    links_listedPerURL.append(row_links[1])
                    print
    #Uncertain if these need to be returned or not
    return(url_total,ranks_total,links_total,links_listedPerURL)
        

(url_total,ranks_total,links_total,links_listedPerURL)=extract_queried_urls_ranks_links(res,url_total,ranks_total,links_total,links_listedPerURL)   

#print links_listedPerURL[1]
