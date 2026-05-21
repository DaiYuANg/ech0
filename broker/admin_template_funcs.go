package broker

func adminTemplateDict(values ...any) map[string]any {
	out := make(map[string]any, len(values)/2)
	for index := 0; index+1 < len(values); index += 2 {
		key, ok := values[index].(string)
		if ok {
			out[key] = values[index+1]
		}
	}
	return out
}
