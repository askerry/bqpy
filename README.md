#bqpy
#####created by Amy Skerry, July 2015

###Goal:
bqpy aims to provide a simple python interface for Google BigQuery datasets. It includes a set of wrappers around the google bigquery api for performing common tasks (listing datasets or tables, copying or deleting tables). It also defines a reference class (BQDF) that provides a barebones pandas dataframe-like interface with a bigquery table.

###Module Description:
- bqpy.core: Set of core functions for querying and fetching data from BigQuery tables
- bqpy.bqdf: Defines BQDF class which provides a vaguely pandas-esque interface with a specific bigquery table. Aims to make simple operations (e.g. getting min or mean of a column, plotting a histogram of a column) as painless as they are in pandas. Note: some of this may become obsolete when https://github.com/pydata/pandas/blob/master/pandas/io/gbq.py is fully developed and stable (for now gbq is experimental and only provides a very basic read/write api).
- bqpy.util: Basic utility functions/wrappers around the bigquery api. Much of this is modeled after the bigquery documentation:
  - https://cloud.google.com/bigquery/docs/managing_jobs_datasets_projects
  - https://cloud.google.com/bigquery/docs/tables
- bqpy.bqviz: Helper functions that support plotting functionality of BQDF
