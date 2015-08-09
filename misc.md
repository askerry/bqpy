## NOTES ON BIGQUERY IMPLEMENTATION AND PRICING

###Pricing
storage: $0.08/GB/month
streaming inserts: $0.01 per 200 MB (individual rows count as minimum of 1 KB)

####FYIs

- batch imports/ exports and api modifications are FREE
- SELECT COUNT(*) is FREE
- JOIN costs = sizes of selected columns + sizes of join columns (not double
counted)
e.g. SELECT shakes.corpus, wiki.language FROM publicdata:samples.shakespeare AS
shakes JOIN publicdata:samples.wikipedia AS wiki ON shakes.corpus = wiki.title
--> charged for total size of the shakes.corpus, wiki.language and wiki.title columns
- Charges don't vary depending on what you do with a column (e.g. selecting all valuesfrom column vs. computing aggregate value of the column  vs. performing regular expressionon each value in column are all the same price)
- Charges depend on datatype:
    - STRING =  2 bytes + the UTF-8 encoded string size
    - INTEGER =  8 bytes
    - FLOAT =	8 bytes
    - BOOLEAN =  1 byte
    - TIMESTAMP =	8 bytes
    - RECORD = 0 bytes + the size of the contained fields
    - NULL = 0 bytes

###Temporary Tables
- pybq is currently exploiting the temporary output tables created by query
- unclear how the pricing model works for these (presumably we pay
for their queries but do we pay for their storage?)
- when do these expire? appear to be built with an expiration time 24 hours
  after creation time. but I haven't seen any documentation about the temp
  tables. what garuntees do we have about them?
- don't have full functionality (couldn't add column? or change schema? need to remember/investigate)

###Query caching
BigQuery now remembers values that you've previously computed, saving you time and the cost of recalculating the query. To maintain privacy, queries are cached on a per-user basis. Cached results are only returned for tables that haven't changed since the last query, or for queries that are not dependent on non-deterministic parameters (such as the current time). Reading cached results is free, but each query still counts against the max number of queries per day quota. Query results are kept cached for 24 hours, on a best effort basis. You can disable query caching with the new flag --use_cache in bq, or "useQueryCache" in the API. This feature is also accessible with the new query options on the BigQuery Web UI.

###Keep an eye on udf feature rolling out
http://stackoverflow.com/questions/28182442/udf-in-bigquery-error
