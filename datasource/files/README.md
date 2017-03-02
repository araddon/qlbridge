




TODO
--------------------
* File Paging not all in memory
  - file iterator
    - filesource
      - loadSchema() not loading array of files
      - partition needs to be consistent
* predicate push-down into api with folder path/query.  write as new generator model.
* subscribe to updates
* s3
* Keep better status of pre-fetched files to see if we have "finished" them




		if _, tableExists := m.files[fi.Table]; !tableExists {
			u.Debugf("%p found new table: %q", m, fi.Table)
			m.files[fi.Table] = make([]*FileInfo, 0)
			m.tablenames = append(m.tablenames, fi.Table)
			nextPartId = 0
		}
		if fi.Partition == 0 && m.ss.Conf.PartitionCt > 0 {
			// assign a partition
			fi.Partition = nextPartId
			//u.Debugf("%d found file part:%d  %s", len(m.files[fi.Table]), fi.Partition, fi.Name)
			nextPartId++
			if nextPartId >= m.ss.Conf.PartitionCt {
				nextPartId = 0
			}
		}


		