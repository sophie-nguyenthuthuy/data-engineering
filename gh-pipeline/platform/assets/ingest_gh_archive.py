""" @bruin
name: ingest_gh_archive
type: python
depends: []
@bruin """

import subprocess, sys
subprocess.check_call([
    sys.executable, "../ingestion/dlt_batch.py",
    "--start", "{{ start_date }}",
    "--end",   "{{ end_date }}",
])
