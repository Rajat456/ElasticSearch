from elasticsearch import Elasticsearch
es = Elasticsearch()

doc = {
  "query": {
    "bool" : {
      "must" : {
        "match" : { "Name" : "Sherlock Holmes" }
      },
      "filter": {
        "match" : { "Director" : "Guy Ritchie, Dexter Fletcher" }
      },
      "must_not" : {
        "match" : {
          "Director" : "Martin Scorsese"
        }
      },
      "should" : [
        { "match" : { "release_date" : "27 April 2012" } },
        { "match" : { "release_date" : "8 January 2010" } }
      ],
      "minimum_should_match" : 1,
      "boost" : 1.0
    }
  }
}

mapping = {
    "settings": {
        "number_of_shards": 2,
        "number_of_replicas": 1
    },
    "mappings": {
        "properties": {
            "Name": {
                "type": "text"
            },
            "Email": {
                "type": "keyword"
            },
            "age": {
                "type": "integer"
            },
            "dob": {
                "type": "date"
            }
        }
    }
}

doc2 = {
    "query": {
        "dis_max" : {
            "queries" : [
                { "match" : { "Name" : "Avengers" }},
                { "match" : { "Director" : "Joss Whedon" }}
            ],
            "tie_breaker" : 0.7
        }
    }
}

# Bool is a way we can combine one or more query parameters together
# you can use Must, must_not , filter, should
# minimum_should_match specify how many minimum conditions needs to be satisfied in should claus
# boost clause increase the score of the doc
# _score specify that this doc relevant to a perform a good search or not
# you can use 'term' or 'range' instead of 'match' according to your needs
res = es.search(index="new-index", body=doc)
print("Total Hits : %d " % res['hits']['total']['value'])
for hit in res['hits']['hits']:
    print("%(Name)s %(Director)s: %(release_date)s" % hit["_source"])


# Using dis_max is another way of combining multiple queries
res = es.search(index="new-index", body=doc2)
print("Total Hits : %d " % res['hits']['total']['value'])
for hit in res['hits']['hits']:
    print("%(Name)s %(Director)s: %(release_date)s" % hit["_source"])


# Mapping is like the schema of the data
# you define what type of data will the fields contains
# text, keyword, date, long, double, boolean etc are different types provided in elastic_search
# you cannot change the mapping of the Index once created but you can reindex the data with new mapping
# the code below will give you the mapping of existing Index
res = es.indices.get_mapping(index="new-index")
print(res)


# you can create index any way mentioned earlier
# just have to pass the mapping into body parameter
# if the index already exist it will throw an error
# python can't handle those errors and will stop the execution, using ignore=400 programs goes to next step
res = es.index(index="test", doc_type='employee', body=mapping, ignore=400)
print(res)

