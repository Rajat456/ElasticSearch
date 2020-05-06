from elasticsearch import Elasticsearch, helpers
es = Elasticsearch()

doc1 = {
    'Name': 'Argo',
    'Director': 'Ben Affleck',
    'release_date': '19 October 2012',
}

doc2 = {
    'Name': 'Apollo 13',
    'Director': 'Ron Howard',
    'release_date': '22 June 1995',
}

doc3 = {
    'Name': 'Lord of the Rings',
    'Director': ' Peter Jackson',
    'release_date': '15 March 2002',
}

final_docs = []


def generate():
    docs = [doc1, doc2, doc3]
    for doc in docs:
        mydoc = {
            "_index": "myindex",
            "_source": doc
        }
        final_docs.append(mydoc)


generate()

# Bulk helps large amount of data to be index, create, delete, and update at once
# you can create a list of all the docs you want to upload and send that list in bulk as a parameter
res = helpers.bulk(es,final_docs)
print(res)


def gen():
    docs = [doc1, doc2, doc3]
    for doc in docs:
        yield {
            "_index": "myindex",
            "_source": doc
        }


# This is another way of using bulk
# gen() will yield a doc one by one and it will indexed
res = helpers.bulk(es, gen())
print(res)
