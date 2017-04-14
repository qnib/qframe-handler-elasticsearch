package qframe_handler_elasticsearch

import (
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/OwnLocal/goes"
	"github.com/zpatrick/go-config"
	"github.com/qnib/qframe-types"
	"github.com/qnib/qframe-utils"
)

const (
	version = "0.1.1"
)

// Elasticsearch holds a buffer and the initial information from the server
type Elasticsearch struct {
	qtypes.Plugin
	buffer 			chan qtypes.QMsg
	indexPrefix string
	indexName		string
	last   			time.Time
	conn 	 		*goes.Connection
}

// NewElasticsearch returns an initial instance
func NewElasticsearch(qChan qtypes.QChan, cfg config.Config, name string) Elasticsearch {
	p := Elasticsearch{
		Plugin: qtypes.NewPlugin(qChan, cfg),
		buffer: make(chan qtypes.QMsg, 1000),
	}
	p.Name = name
	p.Version = version
	nameSplit := strings.Split(p.Name, "_")
	idxDef := p.Name
	if len(nameSplit) != 0 {
		idxDef = nameSplit[len(nameSplit)-1]
	}
	idx, _ := cfg.StringOr(fmt.Sprintf("handler.%s.index-prefix", name), idxDef)
	p.indexPrefix = idx
	p.last = time.Now().Add(-24*time.Hour)
	return p
}

// Takes log from framework and buffers it in elasticsearch buffer
func (eo *Elasticsearch) pushToBuffer() {
	bg := eo.QChan.Data.Join()
	inStr, err := eo.Cfg.String(fmt.Sprintf("handler.%s.inputs", eo.Name))
	if err != nil {
		inStr = ""
	}
	inputs := strings.Split(inStr, ",")
	for {
		val := bg.Recv()
		switch val.(type) {
		case qtypes.QMsg:
			msg := val.(qtypes.QMsg)
			if len(inputs) != 0 && !qutils.IsInput(inputs, msg.Source) {
				continue
			}
			eo.buffer <- msg
		}
	}
}

func (eo *Elasticsearch) createESClient() (err error) {
	host, _ := eo.Cfg.StringOr(fmt.Sprintf("handler.%s.host", eo.Name), "localhost")
	port, _ := eo.Cfg.StringOr(fmt.Sprintf("handler.%s.port", eo.Name), "9200")
	now := time.Now()
	eo.indexName = fmt.Sprintf("%s-%04d-%02d-%02d", eo.indexPrefix, now.Year(), now.Month(), now.Day())
	eo.conn = goes.NewConnection(host, port)
	return
}

func (eo *Elasticsearch) createIndex() (err error) {
	idxCfg := map[string]interface{}{
  	"settings": map[string]interface{}{
      "index.number_of_shards":   1,
      "index.number_of_replicas": 0,
    },
    "mappings": map[string]interface{}{
      "_default_": map[string]interface{}{
        "_source": map[string]interface{}{
          "enabled": true,
        },
        "_all": map[string]interface{}{
          "enabled": false,
        },
      },
    },
	}
	indices := []string{eo.indexName}
	idxExist, _ := eo.conn.IndicesExist(indices)
	if idxExist {
		log.Printf("[DD] Index '%s' already exists", eo.indexName)
		return err
	}
	log.Printf("[DD] Index '%v' does not exists", indices)
	_, err = eo.conn.CreateIndex(eo.indexName, idxCfg)
	if err != nil {
		log.Printf("[WW] Index '%s' could not be created", eo.indexName)
		return err
	}
	log.Printf("[DD] Created index '%s'.", eo.indexName)
	return err
}

func (eo *Elasticsearch) indexDoc(msg qtypes.QMsg) error {
	now := time.Now()
	if eo.last.Day() != now.Day() {
		eo.indexName = fmt.Sprintf("%s-%04d-%02d-%02d", eo.indexPrefix, now.Year(), now.Month(), now.Day())
		eo.createIndex()
		eo.last = now
	}
	d := goes.Document{
		Index: eo.indexName,
		Type:  "log",
		Fields: map[string]interface{}{
			"_msg_version": msg.qmsgVersion,
			"Timestamp": msg.Time.Format("2006-01-02T15:04:05.999999-07:00"),
			"msg":       msg.Msg,
			"source":    msg.Source,
			"type":      msg.Type,
			"host":      msg.Host,
			"Level":     msg.Level,
			"kv":		 msg.KV,
		},
	}
	extraArgs := make(url.Values, 1)
	//extraArgs.Set("ttl", "86400000")
	response, err := eo.conn.Index(d, extraArgs)
	_ = response
	//fmt.Printf("%s | %s\n", d, response.Error)
	return err
}

// Run pushes the logs to elasticsearch
func (eo *Elasticsearch) Run() {
	log.Printf("[II] Start elasticsearch handler: %sv%s",eo.Name, version)
	go eo.pushToBuffer()
	err := eo.createESClient()
	eo.createIndex()
	_ = err
	//cleanEs, _ := eo.Cfg.BoolOr(fmt.Sprintf("handler.%s.remove-index", eo.Name), false)
	for {
		msg := <-eo.buffer
		err := eo.indexDoc(msg)
		if err != nil {
			log.Printf("[EE] Failed to index msg: %s || %v", msg, err)
		}
	}
}
