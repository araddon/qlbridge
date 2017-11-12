
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

* *FileStore* defines file storage Factory (s3, google-storage, local files, sftp, etc)
* *StoreReader* a configured, initilized instance of a specific *FileStore*.  
  File storage reader(writer) for finding lists of files, and opening files.
* *FileHandler* FileHandlers handle processing files from StoreReader, developers
  Register FileHandler implementations in Registry.  FileHandlers will create
  `FileScanner` that iterates rows of this file.
* *FileScanner* File Row Reading, how to transform contents of
  file into *qlbridge.Message* for use in query engine.
  Currently CSV, Json types.

Example: Query CSV Files
----------------------------
We are going to create a CSV `database` of Baseball data from 
http://seanlahman.com/baseball-archive/statistics/

```sh
# download files to local /tmp
mkdir -p /tmp/baseball
cd /tmp/baseball
curl -Ls http://seanlahman.com/files/database/baseballdatabank-2017.1.zip > bball.zip
unzip bball.zip

mv baseball*/core/*.csv .
rm bball.zip
rm -rf baseballdatabank-*

# run a docker container locally
docker run -e "LOGGING=debug" --rm -it -p 4000:4000 \
  -v /tmp/baseball:/tmp/baseball \
  gcr.io/dataux-io/dataux:latest


```
In another Console open Mysql:
```sql
# connect to the docker container you just started
mysql -h 127.0.0.1 -P4000


-- Now create a new Source
CREATE source baseball WITH {
  "type":"cloudstore", 
  "schema":"baseball", 
  "settings" : {
     "type": "localfs",
     "format": "csv",
     "path": "baseball/",
     "localpath": "/tmp"
  }
};

show databases;

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




