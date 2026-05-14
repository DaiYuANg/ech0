package store

import collectionlist "github.com/arcgolabs/collectionx/list"

func cloneRecords(records []Record) []Record {
	copied := collectionlist.NewListWithCapacity[Record](len(records))
	for _, record := range records {
		copied.Add(cloneRecord(record))
	}
	return copied.Values()
}

func cloneGroupPartitionAssignments(assignments []GroupPartitionAssignment) []GroupPartitionAssignment {
	return collectionlist.NewList(assignments...).Values()
}
