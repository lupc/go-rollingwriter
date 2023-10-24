package rollingwriter

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron"
)

type manager struct {
	thresholdSize int64
	startAt       time.Time
	fire          chan string
	cr            *cron.Cron
	context       chan int
	wg            sync.WaitGroup
	lock          sync.Mutex
	rollingNum    int32 //滚动编号

	lastBaseFile string // 最后日志文件（不滚动的）
	lastFile     string //最后记录的日志文件

}

// NewManager generate the Manager with config
func NewManager(c *Config) (Manager, error) {
	m := &manager{
		startAt:    time.Now(),
		cr:         cron.New(),
		fire:       make(chan string),
		context:    make(chan int),
		wg:         sync.WaitGroup{},
		rollingNum: 0,
	}

	//查找相同名称的文件计算出rollingNum
	if m.lastBaseFile == "" {
		m.lastBaseFile = LogFilePath(c)
		m.lastFile = m.lastBaseFile
	}
	var fpath = m.lastBaseFile
	var ext = filepath.Ext(fpath)
	var mpath = strings.ReplaceAll(fpath, ext, fmt.Sprintf("_*%s", ext))
	var matchFiles, err = filepath.Glob(mpath)
	if err == nil {
		m.rollingNum = int32(len(matchFiles))
	}

	// start the manager according to policy
	switch c.RollingPolicy {
	default:
		fallthrough
	case WithoutRolling:
		return m, nil
	case TimeRolling:
		if err := m.cr.AddFunc(c.RollingTimePattern, func() {
			var fname, isSuc = m.GenLogFileName(c)
			if isSuc {
				m.fire <- fname
			}

		}); err != nil {
			return nil, err
		}
		m.cr.Start()
	case VolumeRolling:
		m.ParseVolume(c)
		m.wg.Add(1)
		go func() {
			timer := time.NewTicker(time.Duration(Precision) * time.Second)
			defer timer.Stop()

			// filepath := LogFilePath(c)
			// var file *os.File
			// var err error
			m.wg.Done()

			for {
				select {
				case <-m.context:
					return
				case <-timer.C:
					// if file, err = os.Open(m.lastFile); err != nil {
					// 	continue
					// }
					// if info, err := file.Stat(); err == nil && info.Size() > m.thresholdSize {
					// 	m.fire <- m.GenLogFileName(c)
					// }
					// file.Close()
					var fname, isSuc = m.GenLogFileName(c)
					if isSuc {
						m.fire <- fname
					}
				}
			}
		}()
		m.wg.Wait()
	}
	return m, nil
}

// Fire return the fire channel
func (m *manager) Fire() chan string {
	return m.fire
}

// Close return stop the manager and return
func (m *manager) Close() {
	close(m.context)
	m.cr.Stop()
}

func (m *manager) GetThresholdSize() (size int64) {
	return m.thresholdSize
}

// ParseVolume parse the config volume format and return threshold
func (m *manager) ParseVolume(c *Config) {
	s := strings.ToUpper(c.RollingVolumeSize)
	s = strings.TrimRight(s, "B")

	if !(strings.Contains(string(s), "K") ||
		strings.Contains(string(s), "M") ||
		strings.Contains(string(s), "G") ||
		strings.Contains(string(s), "T")) {

		// set the default threshold with 100M
		m.thresholdSize = 100 * 1024 * 1024
		return
	}

	var unit float64 = 1
	p, _ := strconv.ParseFloat(string(s[:len(s)-1]), 64)
	unitstr := string(s[len(s)-1])

	if s[len(s)-1] == 'B' {
		p, _ = strconv.ParseFloat(string(s[:len(s)-2]), 64)
		unitstr = string(s[len(s)-2:])
	}

	switch unitstr {
	default:
		fallthrough
	case "T", "TB":
		unit *= 1024
		fallthrough
	case "G", "GB":
		unit *= 1024
		fallthrough
	case "M", "MB":
		unit *= 1024
		fallthrough
	case "K", "KB":
		unit *= 1024
	}
	m.thresholdSize = int64(p * unit)
}

func getFileSize(filename string) (int64, error) {
	fi, err := os.Stat(filename)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// GenLogFileName generate the new log file name, filename should be absolute path
func (m *manager) GenLogFileName(c *Config) (logFileRolling string, isSuc bool) {
	// if fileextention is not set, use the default value
	// this line is added to provide backwards compatibility with the current code and unit tests
	// in the next major release, this line should be removed.
	if c.FileExtension == "" {
		c.FileExtension = "log"
	}

	m.lock.Lock()
	// filename = c.fileFormat(m.startAt)

	logFileRolling = ""
	isSuc = false
	LogFile := LogFilePath(c)

	if LogFile == m.lastBaseFile {

		if size, err := getFileSize(m.lastFile); err == nil {

			fmt.Printf("path:%v,size:%v\n", LogFile, size)
			if size > m.thresholdSize {
				m.rollingNum++
				//新文件和最后文件名称相同，则滚动 lastFileName_n.ext
				var ext = filepath.Ext(LogFile)
				var rolExt = fmt.Sprintf("_%d%s", m.rollingNum, ext)
				logFileRolling = strings.TrimRight(LogFile, ext) + rolExt
				m.lastFile = logFileRolling
				isSuc = true
			}
		}

	} else {
		m.rollingNum = 0
	}
	m.lastBaseFile = LogFile
	// reset the start time to now
	m.startAt = time.Now()
	m.lock.Unlock()
	return
}
