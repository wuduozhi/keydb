package keydb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"
)

var maxSegments = 8

// merge on disk segments for the database
func mergeDiskSegments(db *Database) {
	defer db.wg.Done()
	//defer fmt.Println("merger complete on "+db.path)

	for {
		db.Lock()
		if db.closing || db.err != nil {
			db.Unlock()
			return
		}

		// the following prevents a Close from occurring while this
		// routine is running

		db.wg.Add(1)

		db.Unlock()

		err := mergeDiskSegments0(db, maxSegments)
		if err != nil {
			db.Lock()
			db.err = errors.New("unable to merge segments: " + err.Error())
			db.Unlock()
		}

		db.wg.Done()

		time.Sleep(1 * time.Second)
	}
}

// 合并一个数据库db下面的所有表的diskSegment，单表最多保存的segment数量由segmentCount指定
func mergeDiskSegments0(db *Database, segmentCount int) error {
	db.Lock()
	copy := make([]*internalTable, 0)
	for _, table := range db.tables {
		copy = append(copy, table)
	}
	db.Unlock()

	for _, table := range copy {
		err := mergeTableSegments(db, table, segmentCount)
		if err != nil {
			return err
		}
	}
	return nil
}

// 合并单个表的diskSegmen
func mergeTableSegments(db *Database, table *internalTable, segmentCount int) error {

	var index = 0

	for {

		table.Lock()
		segments := table.segments
		table.Unlock()

		// 表包含的segment数量小于最大值，不需要合并
		if len(segments) <= segmentCount {
			return nil
		}

		// 一次最多合并的segment数量，默认为segment总数的一半
		maxMergeSize := len(segments) / 2
		if maxMergeSize < 4 {
			maxMergeSize = 4
		}

		// ensure that only valid disk segments are merged
		mergable := make([]*diskSegment, 0)

		for _, s := range segments[index:] {
			ds, ok := s.(*diskSegment)

			if ok {
				mergable = append(mergable, ds)
				if len(mergable) == maxMergeSize {
					break
				}
			} else {
				break
			}
		}

		// 合并到最后1个文件,将游标回滚，重新开始合并操作
		if len(mergable) < 2 {
			index = 0
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// 最后一个segment的id, 由于segments本身有序，最后一个segment即是最新的segment
		id := mergable[len(mergable)-1].id
		segments = segments[index : index+len(mergable)]

		newseg, err := mergeDiskSegments1(db.path, table.name, id, segments)
		if err != nil {
			return err
		}

		table.Lock()
		for table.transactions > 0 {
			table.Unlock()
			time.Sleep(100 * time.Millisecond)
			table.Lock()
		}

		segments = table.segments

		for i, s := range mergable {
			if s != segments[i+index] {
				return errors.New(fmt.Sprint("unexpected segment change,", s, segments[i]))
			}
		}

		for _, s := range mergable {
			err0 := s.keyFile.Close()
			err1 := s.dataFile.Close()
			err2 := os.Remove(s.keyFile.Name())
			err3 := os.Remove(s.dataFile.Name())

			err := errn(err0, err1, err2, err3)
			if err != nil {
				return err
			}
		}

		/*将合并的segment放回segments, 替换掉被合并的mergable的位置, 保持合并前后的顺序一直*/
		newsegments := make([]segment, 0)

		newsegments = append(newsegments, segments[:index]...)
		newsegments = append(newsegments, newseg)
		newsegments = append(newsegments, segments[index+len(mergable):]...)

		table.segments = newsegments

		index++
		table.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

var mergeSeq uint64

// 将多个segment合并到一个diskSegment
func mergeDiskSegments1(dbpath string, table string, id uint64, segments []segment) (segment, error) {

	base := filepath.Join(dbpath, table+".merged.")

	sid := strconv.FormatUint(id, 10)

	seq := atomic.AddUint64(&mergeSeq, 1)
	sseq := strconv.FormatUint(seq, 10)

	keyFilename := base + "." + sseq + ".keys." + sid
	dataFilename := base + "." + sseq + ".data." + sid

	ms := newMultiSegment(segments)
	itr, err := ms.Lookup(nil, nil)
	if err != nil {
		return nil, err
	}

	return writeAndLoadSegment(keyFilename, dataFilename, itr)

}
