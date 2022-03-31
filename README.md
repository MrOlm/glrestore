# glrestore (Glacier Restore)

Easily restore objects from AWS S3 glacier from the command line. Built using python

## Installation options

### pip
```
$ pip install glrestore
```

## Quick start

### Show program help and modules:
```
$ glrestore -h
```

### Example command to restore some files for 7 days as quickly as possible:
```
$ glrestore -f s3://cool-bucket/users/mattolm/archived-*.csv -d 7 -s Expedited
```