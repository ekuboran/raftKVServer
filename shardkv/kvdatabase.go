package shardkv

type KVDatabase struct {				//KV数据库
	Database map[string]string
}

func (kvdb *KVDatabase) Get(key string) (value string, ok bool) {
	if value, ok = kvdb.Database[key]; ok {
		return value, ok
	}
	return "", ok
}

func (kvdb *KVDatabase) Put(key string, value string) (newvalue string) {
	kvdb.Database[key] = value
	return value
}


func (kvdb *KVDatabase) Append(key string, args string) (newvalue string) {
	if _, ok := kvdb.Database[key]; ok {
		kvdb.Database[key] += args
		return kvdb.Database[key]
	}
	kvdb.Database[key] = args
	return args
}