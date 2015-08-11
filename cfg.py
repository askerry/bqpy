gsdk_path = '/Users/amyskerry/google-cloud-sdk/platform/bq'

MAX_ROWS = 10000000  # max rows to ever return locally
DEBUG = False
CACHE_MAX = 2  # megabytes to store in local cache
FETCH_BY_DEFAULT = False

# TODO setup basic access control so that you don't perform operations
# that modify table unless you mean to
WRITEABLE = True
WRITE_ACCESS = []
STORAGE_BUCKET = 'loml'
