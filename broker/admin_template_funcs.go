package broker

import collectionmapping "github.com/arcgolabs/collectionx/mapping"

func adminTemplateDict(values ...any) map[string]any {
	out := collectionmapping.NewMap[string, any]()
	for index := 0; index+1 < len(values); index += 2 {
		key, ok := values[index].(string)
		if ok {
			out.Set(key, values[index+1])
		}
	}
	return out.All()
}
