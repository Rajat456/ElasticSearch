from elasticsearch import Elasticsearch
es = Elasticsearch()

doc = {
    'Name': 'Sherlock Holmes',
    'Director': 'Guy Ritchie, Dexter Fletcher',
    'release_date': '8 January 2010',
}

# Creating a New Index
# name of the index should be lowercase otherwise it will throw an error
# Index acts as a (Table/collection of Similar data) if you want to compare elastic search with any RDBMS
res = es.indices.create(index="new-index", ignore=400)
print(res)

# This is another way off creating a new Index
# If the index exist it will insert/update the data otherwise first it will create the new index then insert the data
# id acts as a row
# if doc_type is same this means all that data is similar
# if the id already exist it will update that id with the new data otherwise create a new one
res = es.index(index="new-index", doc_type='Movie', id=1, body=doc)
print(res)


# get_alias method is used to get indices from ElasticSearch
# '*' will result in all the indices
# if you want to check for a particular name replace '*' with the indices name
# if it exist it will give the result other wise it will throw index_not_found_exception
res = es.indices.get_alias("*")
print(res)

# This retrieves the data from the index and id mentioned
res = es.get(index="new-index", id=1)
print(res)


# Deleting the Index
res = es.indices.delete(index='new-index', ignore=[400, 404])
print(res)

# Deleting a particular doc
res = es.delete(index='new-index', id=1)
print(res)

# The Search Method is used to query the a particular index
# The next line is similar like 'Like or where' clause in sql query but not exactly
# when you store a field it breaks into token 'Sherlock Holmes' will become [sherlock, Holmes]
# searching with match also does the same thing it breaks the input provided into token and then it search those tokens
# so if you provide either sherlock or holmes will produce the same result
# try this if you want all the result in the index
# es.search(index="new-index", body={"query": {"match_all":{}}})
res = es.search(index="new-index", body={"query": {"match": {'Name':'Sherlock Holmes'}}})
print("Total Hits : %d " % res['hits']['total']['value'])
for hit in res['hits']['hits']:
    print("%(Name)s %(Director)s: %(release_date)s" % hit["_source"])


# Term is another way of querying the index
# Although Term does not work against where field type is text
# Term looks for an exact value unlike match which breaks the provided input into token and searches each token
# That's why result will be either found with the exact value or not
# the next line does not produces any result as we are searching against a text value field
res = es.search(index="new-index", body={"query": {"term": {"Name": {"value": "Sherlock Holmes"}}}})
print("Total Hits : %d " % res['hits']['total']['value'])
for hit in res['hits']['hits']:
    print("%(Name)s %(Director)s: %(release_date)s" % hit["_source"])




