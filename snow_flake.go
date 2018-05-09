package awesome_id

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	// 起始时间戳，用于用当前时间戳减去这个时间戳，算出偏移量
	epoch = 1514736000000

	// workerId 占用的位数5（表示只允许 workId 的范围为：0-1023
	workerIdBits = 5
	// dataCenterId 占用的位数：5 表示只允许 dataCenterId 的范围为：0-1023）
	dataCenterIdBits = 5
	// 序列号占用的位数：12（表示只允许序列号的范围为：0-4095）
	sequenceBits = 12

	// workerId 可以使用的最大数值：31
	maxWorkerId = -1 ^ (-1 << workerIdBits)
	// dataCenterId 可以使用的最大数值：31
	maxDataCenterId = -1 ^ (-1 << dataCenterIdBits)

	// workerId 左移位数
	workerIdLeftShift = sequenceBits
	// dataCenterId 左移位数
	dataCenterIdLeftShift = sequenceBits + workerIdBits
	// 时间戳 左移位数
	timestampLeftShift = sequenceBits + workerIdBits + dataCenterIdBits

	// 用mask防止溢出:位与运算保证计算的结果范围始终是 0-4095
	sequenceMask = -1 ^ (-1 << sequenceBits)

	sequence = 0
	lastTimestamp = -1

	maxIdQuantity = 100
)

type IdWorker struct {
	lock          sync.Mutex
	epoch         int64
	lastTimestamp int64
	dataCenterId  int64
	workerId      int64
	sequence      int64
}

func NewIdWorker(workerId, dataCenterId int64) (iw *IdWorker, err error) {
	if workerId > maxWorkerId || workerId < 0 {
		return nil, errors.New(fmt.Sprintf("worker Id can't be greater than %d or less than 0", maxWorkerId))
	}
	if dataCenterId > maxDataCenterId || dataCenterId < 0 {
		return nil, errors.New(fmt.Sprintf("datacenter Id can't be greater than %d or less than 0", maxDataCenterId))
	}
	iw = &IdWorker{
		epoch:         epoch,
		lastTimestamp: lastTimestamp,
		dataCenterId:  dataCenterId,
		workerId:      workerId,
		sequence:      sequence,
	}
	return iw, nil
}

func (iw *IdWorker) GetId() (int64, error) {
	iw.lock.Lock()
	defer iw.lock.Unlock()
	return iw.nextId()
}

func (iw *IdWorker) GetIds(quantity int) (ids []int64, err error) {
	if quantity > maxIdQuantity || quantity < 0 {
		return nil, errors.New(fmt.Sprintf("quantity can't be greater than %d or less than 0", maxIdQuantity))
	}

	ids = make([]int64, quantity)

	iw.lock.Lock()
	defer iw.lock.Unlock()

	for i := 0; i < quantity; i++ {
		ids[i], _ = iw.nextId()
	}

	return ids, nil
}

func ParseId(id int64) (timestamp, dataCenterId, workerId, sequence int64, t time.Time) {
	timestamp = (id >> timestampLeftShift) + epoch
	dataCenterId = (id >> dataCenterIdLeftShift) & maxDataCenterId
	workerId = (id >> workerIdLeftShift) & maxWorkerId
	sequence = id & sequenceMask
	t = time.Unix(timestamp/1000, (timestamp%1000)*int64(time.Millisecond))
	return
}

func (iw *IdWorker) nextId() (int64, error) {
	timestamp := timeGen()

	// 如果当前时间戳小于上一次 ID 生成的时间戳，说明系统时钟回退过，应抛出异常
	if timestamp < iw.lastTimestamp {
		return 0, errors.New(fmt.Sprintf("Clock moved backwards.  Refusing to generate id for %d milliseconds", iw.lastTimestamp-timestamp))
	}

	// 解决跨毫秒生成 ID 序列号始终为偶数的缺陷:如果是同一时间生成的，则进行毫秒内序列
	if timestamp == iw.lastTimestamp {
		// 通过位与运算保证计算的结果范围始终是 0-4095
		iw.sequence = (iw.sequence + 1) & sequenceMask
		if iw.sequence == 0 {
			timestamp = tilNextMillis(timestamp)
		}
	} else {
		// 时间戳改变，毫秒内序列重置
		iw.sequence = 0
	}

	iw.lastTimestamp = timestamp

	/*
	 * 1.左移运算是为了将数值移动到对应的段(41、5、5，12那段因为本来就在最右，因此不用左移)
	 * 2.然后对每个左移后的值(la、lb、lc、sequence)做位或运算，是为了把各个短的数据合并起来，合并成一个二进制数
	 * 3.最后转换成10进制，就是最终生成的id
	 */
	return ((timestamp - iw.epoch) << timestampLeftShift) |
		(iw.dataCenterId << dataCenterIdLeftShift) |
		(iw.workerId << workerIdLeftShift) |
		iw.sequence, nil
}

func timeGen() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

/*
保证返回的毫秒数在参数之后(阻塞到下一个毫秒，直到获得新的时间戳)
 */
func tilNextMillis(lastTimestamp int64) int64 {
	timestamp := timeGen()
	for timestamp <= lastTimestamp {
		timestamp = timeGen()
	}
	return timestamp
}
