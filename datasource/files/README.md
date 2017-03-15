
File Data Source
---------------------------------

Turn files into a SQL Queryable data source.

Allows Cloud storage (Google Storage, S3, etc) to be queried with traditional 
sql.  Also allows these files to have custom serializations, compressions,
encryptions.

**Design Goal**
* *Hackable Stores* easy to add to Google Storage, local files, s3, etc.
* *Hackable File Formats* Protbuf files, WAL files, mysql-bin-log's etc.


**Developing new Sources**

* *FileStore* defines file storage (s3, google-storage, local files, sftp, etc)
  * *StoreReader* defines file storage reader(writer) for finding lists of files, and
    opening files.
* *FileHandler* Defines Registry to create `FileScanner`. Also for 
    extracting info from path info, ie often folders serve as "columns" in the virtual table.
  * *FileScanner* File Row Reading, how to transform contents of
    file into *qlbridge.Message* for use in query engine.
    Currently CSV, Json types.  


TODO
----------------------------

* implement event-notification
* SFTP
* Change the store library?  Currently https://github.com/lytics/cloudstorage but consider:
  * https://github.com/graymeta/stow
  * https://github.com/rook/rook
  * https://minio.io/
  * http://rclone.org




