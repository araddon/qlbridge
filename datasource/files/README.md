
File Data Source
---------------------------------

Turn files into a SQL Queryable data source.

Allows Cloud storage (Google Storage, S3, etc) files (csv, json, custom-protobuf)
to be queried with traditional sql.  Also allows these files to have custom 
serializations, compressions, encryptions.

**Design Goal**
* *Hackable Stores* easy to add to Google Storage, local files, s3, etc.
* *Hackable File Formats* Protbuf files, WAL files, mysql-bin-log's etc.


**Developing new stores or file formats**

* *FileStore* defines file storage (s3, google-storage, local files, sftp, etc)
  * *StoreReader* defines file storage reader(writer) for finding lists of files, and
    opening files.
* *FileHandler* Defines Registry to create handler for converting a file from `StoreReader`
    into a `FileScanner` that iterates rows of this file. Also extracts info from 
    filepath, ie often folders serve as "columns" or "tables" in the virtual table.
  * *FileScanner* File Row Reading, how to transform contents of
    file into *qlbridge.Message* for use in query engine.
    Currently CSV, Json types.  

Example
----------------------------

The default example dataux docker container
includes a single csv "file/table".
```sh

docker pull gcr.io/dataux-io/dataux:latest
docker run --rm -e "LOGGING=debug" -p 4000:4000 --name dataux gcr.io/dataux-io/dataux:latest

# delete it
docker -D run gcr.io/dataux-io/dataux:latest

```

![dataux_file_source](https://cloud.githubusercontent.com/assets/7269/23976158/12a378be-09a3-11e7-971e-8a05d7002aaf.png)

```sql
mysql -h 127.0.0.1 -P4000

use baseball;

show tables;

describe appearances

select count(*) from appearances;

select * from appearances limit 10;

```

TODO
----------------------------

* implement event-notification
* SFTP
* Change the store library?  Currently https://github.com/lytics/cloudstorage but consider:
  * https://github.com/graymeta/stow
  * https://github.com/rook/rook
  * https://minio.io/
  * http://rclone.org




