package shardctrler

import(
	"sort"
)

// 执行join操作
func (sc *ShardCtrler) executeJoin(servers map[int][]string)  {
	length := len(sc.configs)
	lastConfig := sc.configs[length-1]
	newGroups := deepCopy(lastConfig.Groups)

	var tmpgid int						// 先将未分配的shard分配到这个group中
	for gid, servers := range servers {		// add a set of groups 
		newGroups[gid] = servers
		tmpgid = gid
	}
	newConfig := Config{			// 构建新的configuration
		Num: length,
		Groups: newGroups,
	}
	
	// 先将未分配的shard(即分配给group 0)分配给已有的group
	for shard, gid := range lastConfig.Shards {
		if gid == 0 {
			lastConfig.Shards[shard] = tmpgid
		}
	}

	// group到shards的映射(这时无效group 0应该不包含任何shard, 因此g2s中没有key=0)
	g2s := getGroupToShards(newGroups, lastConfig.Shards)

	// 接下来做负载均衡，尽可能均匀地划分分片
	// gid为0时不包含任何分组，并且初始时所有未分配的shard应分配给 gid0
	// 从shard多的gruop以及无效分组group0中往shard少的group中迁移shard
	for {
		source, target := GetGidWithMaxShards(g2s), GetGidWithMinShards(g2s)
		if len(g2s[source]) - len(g2s[target]) <= 1{			// 当多和少的shard数量相差小于等于1时停止
			break
		}
		g2s[target] = append(g2s[target], g2s[source][0])
		g2s[source] = g2s[source][1:]
	}

	// 根据更新过的g2s构建新的Shard->gid的映射
	newShards := [NShards]int{}
	for gid, shards := range g2s {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newShards = sortShards(newShards)
	newConfig.Shards = newShards

	sc.configs = append(sc.configs, newConfig)
}


// 执行leave操作
func (sc *ShardCtrler) executeLeave(gids []int)  {
	length := len(sc.configs)
	lastConfig := sc.configs[length-1]
	newGroups := deepCopy(lastConfig.Groups)
	newConfig := Config{			// 构建新的configuration
		Num: length,
		Groups: newGroups,
	}
	
	g2s := getGroupToShards(newGroups, lastConfig.Shards)
	orphanShards := make([]int, 0)			// 用于保存被移除的组中的Shards
	for _, gid := range gids {
		// delete a set of groups
		delete(newConfig.Groups, gid)
		// 同时也要修改group->shards的映射
		if shards, ok := g2s[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(g2s, gid)
		}
	}

	// 如果直接删没了
	if len(g2s) == 0{
		newConfig.Shards = [10]int{}
		sc.configs = append(sc.configs, newConfig)
		return
	}

	// 接下来做平衡，将被移除的组中的Shards划分到较少shard的组中
	for _, shard := range orphanShards {
		target := GetGidWithMinShards(g2s)
		g2s[target] = append(g2s[target], shard)
	}
	// 构建新的Shard->gid的映射
	newShards := [NShards]int{}
	for gid, shards := range g2s {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newShards = sortShards(newShards)
	newConfig.Shards = newShards

	sc.configs = append(sc.configs, newConfig)
}

// 执行move操作
func (sc *ShardCtrler) executeMove(shard int, gid int)  {
	// 创建新的配置，将该shard分配给该gid的group
	length := len(sc.configs)
	lastConfig := sc.configs[length-1]
	newGroups := deepCopy(lastConfig.Groups)
	newConfig := Config{
		Num: length,
		Shards: lastConfig.Shards,
		Groups: newGroups,
	}
	newConfig.Shards[shard] = gid
	sc.configs = append(sc.configs, newConfig)
}


// 执行query操作
func (sc *ShardCtrler) executeQuery(num int) Config {
	length := len(sc.configs)
	config := Config{}
	if num == -1 || num >= length{		// 数字为-1或大于已知的最大配置数回复最新配置
		config = sc.configs[length-1]
	} else {
		config = sc.configs[num]
	}
	newGroups := deepCopy(config.Groups)
	newConfig := Config{
		Num: config.Num,
		Shards: config.Shards,
		Groups: newGroups,			// 不要直接传config.Groups，因为map类型进行传递为引用传递
	}
	return newConfig
}


// 复制groups
func deepCopy(gropus map[int][]string) map[int][]string {
	copygropus := make(map[int][]string)
	for k, v := range gropus {
		copygropus[k] = v
	}
	return copygropus
}

// 获取group->shards的映射
func getGroupToShards(groups map[int][]string, shards [NShards]int) map[int][]int {
    groupToShards := make(map[int][]int)

    // 初始化群组到分片的映射
    for gid := range groups {
        groupToShards[gid] = []int{}
    }

    // 根据现有的分片分配情况更新映射
    for shard, gid := range shards {
        if gid != 0 {
            groupToShards[gid] = append(groupToShards[gid], shard)
        }
    }

    return groupToShards
}


// 获取group对Shards的映射中shard最少的group id
func GetGidWithMinShards(g2s map[int][]int) int {
	index, minNum := -1, NShards+1
	for gid := range g2s {
		if len(g2s[gid]) < minNum {		// gid为0不会为最少的，因为初始时未分配的shard都分配给了gid 0
			index, minNum = gid, len(g2s[gid])
		}
	}
	return index
}

// 获取group对Shards的映射中shard最多的group id
func GetGidWithMaxShards(g2s map[int][]int) int {
	// 只要gid0还有shard，就去gid0往其他group迁移shard
	// if shards, ok := g2s[0]; ok && len(shards) > 0 {		
	// 	return 0
	// }
	index, maxNum := -1, -1
	for gid := range(g2s) {
		if len(g2s[gid]) > maxNum {
			index, maxNum = gid, len(g2s[gid])
		}
	}
	return index
}


// 对Shards排序
func sortShards(shards [NShards]int) [NShards]int {
	slice := shards[:]
	sort.Ints(slice)
	for i := range slice {
		shards[i] = slice[i]
	}
	return shards
}