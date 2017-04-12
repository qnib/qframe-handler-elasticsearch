package qframe_handler_elasticsearch

import (
	"net/url"
	"time"
	"github.com/zpatrick/go-config"
	"log"

	"github.com/qnib/qframe-types"
	"github.com/OwnLocal/goes"
)

const (
	version = "0.1.1"
)
// Elasticsearch holds a buffer and the initial information from the server
type Elasticsearch struct {
	qtypes.Plugin
	buffer chan qtypes.QMsg
}

// NewElasticsearch returns an initial instance
func NewElasticsearch(qChan qtypes.QChan, cfg config.Config, name string) Elasticsearch {
	p := Elasticsearch{
		Plugin: qtypes.NewPlugin(qChan, cfg),
		buffer: make(chan qtypes.QMsg, 1000),
	}
	p.Name = name
	p.Version = version
	return p
}

// Takes log from framework and buffers it in elasticsearch buffer
func (eo *Elasticsearch) pushToBuffer() {
	bg := eo.QChan.Data.Join()
	for {
		val := bg.Recv()
		switch val.(type) {
		case qtypes.QMsg:
			log := val.(qtypes.QMsg)
			eo.buffer <- log
		}
	}
}

func (eo *Elasticsearch) createESClient() (conn *goes.Connection) {
	host, err := eo.Cfg.StringOr("handler.elasticsearch.host", "localhost")
	if err != nil {
		panic(err)
	}
	port, err := eo.Cfg.StringOr("handler.elasticsearch.port", "9200")
	if err != nil {
		panic(err)
	}
	conn = goes.NewConnection(host, port)
	return
}

func createIndex(conn *goes.Connection) (err error) {
	l := NewLogstash(1,0)
	idxCfg, err := l.GetConfig()
	if err != nil {
		return err
	} else {
		resp, err := conn.CreateIndex("logstash-2016-11-27", idxCfg)
		if err != nil {
			log.Printf("[EE] Creating index: Response:%s || %v", resp, err)
			return err
		}

	}
	return err
}

func indexDoc(conn *goes.Connection, msg qtypes.QMsg) error {
	now := time.Now()
	d := goes.Document{
		Index: "logstash-2016-11-27",
		Type:  "log",
		Fields: map[string]interface{}{
			"Timestamp": now.Format("2006-01-02T15:04:05.999999-07:00"),
			"msg":       msg.Msg,
			"source":    msg.Source,
			"type":      msg.Type,
			"host":      msg.Host,
		},
	}
	extraArgs := make(url.Values, 1)
	//extraArgs.Set("ttl", "86400000")
	response, err := conn.Index(d, extraArgs)
	_ = response
	//fmt.Printf("%s | %s\n", d, response.Error)
	return err
}

// Run pushes the logs to elasticsearch
func (eo Elasticsearch) Run() {
	log.Printf("[II] Start elasticsearch handler v%s", version)
	go eo.pushToBuffer()
	conn := eo.createESClient()
	createIndex(conn)
	for {
		msg := <-eo.buffer
		err := indexDoc(conn, msg)
		if err != nil {
			log.Printf("[EE] Failed to index msg: %s || %v", msg, err)
		}
	}
}
