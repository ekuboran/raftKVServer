package raft

// log entry对象
type Entry struct {
	Command interface{}			// 用于状态机的命令
	Term int					// leader接收到该条目时的任期号
	Index int					// leader接受该条目的index
}

// log对象
type Log struct {
	Entries []Entry				// log的日志条目数组
}

// 获取某个位置(日志条目包含的index)的条目的指针
func (l *Log) getEntry(index int) *Entry {
	realindex := l.searchRealIndex(index)
	return &l.Entries[realindex]
}

// 截断日志（保留前半部分，不包括index处）
func (l *Log) truncate(index int) {
	l.Entries = l.Entries[:index]
}

// 裁剪日志(保留后半部分，包括index处)。
func (l *Log) trim(index int) []Entry {
	entries := make([]Entry, l.len()-index)
	copy(entries, l.Entries[index:])
	return entries
}
// 不要这么写，lab要求Raft必须以允许Go垃圾收集器释放和重用内存的方式丢弃旧日志条目，而这样写切片仍然引用原始底层数组的一部分内存，只是切片的长度和容量被修改了
// func (l *Log) trim (index int) {
// 	l.Entries = l.Entries[index:]
// }

// 获取日志的长度
func (l *Log)len() int {
	return len(l.Entries)
}

// 获取日志的最后一个条目的指针
func (l *Log)lastLog() *Entry {
	return &l.Entries[l.len()-1]
}

// Append 日志条目到日志中
func (l *Log) append(entries ...Entry)  {
	l.Entries = append(l.Entries, entries...)
}

// 创建空的日志
func makeEmptyLog() Log {
	log := Log{
		Entries : make([]Entry, 0),
	}
	return log
}

// 查找在Entries[]数组中的真实索引(参数为条目内包含的索引)。若没找到，则返回最后的索引+1
// 裁剪日志之后，Entries[]数组本身的索引就和条目内包含的索引不匹配了。
func (l *Log) searchRealIndex(index int) int {
	left, right := 0, l.len()-1
	for left <= right {
		mid := left + (right-left)/2
		if l.Entries[mid].Index == index {
			return mid
		} else if l.Entries[mid].Index < index {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return l.len()
}

