# download_papertrail

A utility to download archive logs from Papertrail.
```
Usage: download_papertrail [OPTIONS] --api_token <api_token> [FILES]...

Arguments:
  [FILES]...  Which archive files to download, in the format "YYYY-MM-DD-HH"

Options:
      --api_token <api_token>
          API key for Papertrail [env: PAPERTRAIL_API_TOKEN]
  -c, --concurrency <CONCURRENCY>
          How many files to download at once [default: # of logical cores, 4 if unavailable]
  -o, --out <OUT>
          Where to download the files [default: .]
  -t, --throttle-duration <THROTTLE_DURATION>
          How long in milliseconds to wait in between requests [default: 200]
  -h, --help
          Print help
  -V, --version
          Print version
```

## TODO:
- Allow passing any datetime-like value for a start and end range
- Optionally deflate, parse, and combine the downloaded files into more useful formats
