package qframe_handler_elasticsearch

import (
	"net/url"
	"time"
	"github.com/zpatrick/go-config"
	"log"
	"github.com/OwnLocal/goes"
	"fmt"
	"strings"

	"github.com/qnib/qframe-types"
	"github.com/qnib/qframe-utils"
)

const (
	version = "0.1.1"
)
// Elasticsearch holds a buffer and the initial information from the server
type Elasticsearch struct {
	qtypes.Plugin
	buffer chan qtypes.QMsg
	index string
}

// NewElasticsearch returns an initial instance
func NewElasticsearch(qChan qtypes.QChan, cfg config.Config, name string) Elasticsearch {
	p := Elasticsearch{
		Plugin: qtypes.NewPlugin(qChan, cfg),
		buffer: make(chan qtypes.QMsg, 1000),
	}
	p.Name = name
	p.Version = version
	idx, _ := cfg.StringOr(fmt.Sprintf("handler.%s.index-template", name), "logstash-2016-11-27")
	p.index = 	idx

	return p
}

// Takes log from framework and buffers it in elasticsearch buffer
func (eo *Elasticsearch) pushToBuffer() {
	bg := eo.QChan.Data.Join()
	for {
		val := bg.Recv()
		switch val.(type) {
		case qtypes.QMsg:
			msg := val.(qtypes.QMsg)
			inStr, err := eo.Cfg.String(fmt.Sprintf("handler.%s.inputs", eo.Name))
			if err != nil {
				inStr = ""
			}
			inputs := strings.Split(inStr, ",")
			if len(inputs) != 0 && ! qutils.IsInput(inputs, msg.Source) {
				//fmt.Printf("%s %-7s sType:%-6s sName:%-10s[%d] DROPED : %s\n", qm.TimeString(), qm.LogString(), qm.Type, qm.Source, qm.SourceID, qm.Msg)
				continue
			}
			eo.buffer <- msg
		}
	}
}

func (eo *Elasticsearch) createESClient() (conn *goes.Connection) {
	host, _ := eo.Cfg.StringOr(fmt.Sprintf("handler.%s.host", eo.Name), "localhost")
	port, _ := eo.Cfg.StringOr(fmt.Sprintf("handler.%s.port", eo.Name), "9200")
	conn = goes.NewConnection(host, port)
	return
}

func (eo *Elasticsearch) createIndex(conn *goes.Connection) (err error) {
	l := NewLogstash(1,0)
	idxCfg, err := l.GetConfig()
	if err != nil {
		return err
	} else {
		resp, err := conn.CreateIndex(eo.index, idxCfg)
		if err != nil {
			log.Printf("[EE] Creating index-format '%s': Response:%s || %v", eo.index, resp, err)
			return err
		}

	}
	return err
}

func (eo *Elasticsearch) indexDoc(conn *goes.Connection, msg qtypes.QMsg) error {
	now := time.Now()
	d := goes.Document{
		Index: eo.index,
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
func (eo *Elasticsearch) Run() {
	log.Printf("[II] Start elasticsearch handler v%s", version)
	go eo.pushToBuffer()
	conn := eo.createESClient()
	eo.createIndex(conn)
	for {
		msg := <-eo.buffer
		err := eo.indexDoc(conn, msg)
		if err != nil {
			log.Printf("[EE] Failed to index msg: %s || %v", msg, err)
		}
	}
}
