# tap-googleplay

This is a [Singer](https://singer.io) tap that produces JSON-formatted 
data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md) 
from the Google Play Console reports.

This tap supports multiple apps/package names

Quickstart:
* Grant this account an access to read reports in Google Play Console 

Sample config:
```$json
{
  "key_file": "./credentials.json",
  "start_date": "2022-01-01T00:00:00Z",
  "bucket_name": "pubsite_prod_rev_01234567890987654321",
}
```
