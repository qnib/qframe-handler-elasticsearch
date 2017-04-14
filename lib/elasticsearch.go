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
	version = "0.1.2"
)

// Elasticsearch holds a buffer and the initial information from the server
type Elasticsearch struct {
	qtypes.Plugin
	buffer 		chan qtypes.QMsg
	indexPrefix	string
	indexName	string
	last   		time.Time
	conn 	 	*goes.Connection
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
func (p *Elasticsearch) pushToBuffer() {
	bg := p.QChan.Data.Join()
	inStr, err := p.Cfg.String(fmt.Sprintf("handler.%s.inputs", p.Name))
	if err != nil {
		inStr = ""
	}
	inputs := strings.Split(inStr, ",")
	srcSuccess, _ := p.Cfg.BoolOr(fmt.Sprintf("handler.%s.source-success", p.Name), true)
	for {
		val := bg.Recv()
		switch val.(type) {
		case qtypes.QMsg:
			msg := val.(qtypes.QMsg)
			if len(inputs) != 0 && !qutils.IsLastSource(inputs, msg.Source) {
				continue
			}
			if msg.SourceSuccess != srcSuccess {
				continue
			}
			p.buffer <- msg
		}
	}
}

func (p *Elasticsearch) createESClient() (err error) {
	host, _ := p.Cfg.StringOr(fmt.Sprintf("handler.%s.host", p.Name), "localhost")
	port, _ := p.Cfg.StringOr(fmt.Sprintf("handler.%s.port", p.Name), "9200")
	now := time.Now()
	p.indexName = fmt.Sprintf("%s-%04d-%02d-%02d", p.indexPrefix, now.Year(), now.Month(), now.Day())
	p.conn = goes.NewConnection(host, port)
	return
}

func (p *Elasticsearch) createIndex() (err error) {
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
	indices := []string{p.indexName}
	idxExist, _ := p.conn.IndicesExist(indices)
	if idxExist {
		log.Printf("[DD] Index '%s' already exists", p.indexName)
		return err
	}
	log.Printf("[DD] Index '%v' does not exists", indices)
	_, err = p.conn.CreateIndex(p.indexName, idxCfg)
	if err != nil {
		log.Printf("[WW] Index '%s' could not be created", p.indexName)
		return err
	}
	log.Printf("[DD] Created index '%s'.", p.indexName)
	return err
}

func (p *Elasticsearch) indexDoc(msg qtypes.QMsg) error {
	now := time.Now()
	if p.last.Day() != now.Day() {
		p.indexName = fmt.Sprintf("%s-%04d-%02d-%02d", p.indexPrefix, now.Year(), now.Month(), now.Day())
		p.createIndex()
		p.last = now
	}
	data := map[string]interface{}{
		"msg_version": 	msg.QmsgVersion,
		"Timestamp": 	msg.Time.Format("2006-01-02T15:04:05.999999-07:00"),
		"msg":       	msg.Msg,
		"source":    	msg.Source,
		"source_path":  msg.SourcePath,
		"type":      	msg.Type,
		"host":      	msg.Host,
		"Level":     	msg.Level,
	}
	if len(msg.KV) != 0 {
		data[msg.Source] = msg.KV
	}
	d := goes.Document{
		Index: p.indexName,
		Type:  "log",
		Fields: data,
	}
	extraArgs := make(url.Values, 1)
	//extraArgs.Set("ttl", "86400000")
	response, err := p.conn.Index(d, extraArgs)
	_ = response
	//fmt.Printf("%s | %s\n", d, response.Error)
	return err
}

// Run pushes the logs to elasticsearch
func (p *Elasticsearch) Run() {
	log.Printf("[II] Start elasticsearch handler: %sv%s", p.Name, version)
	go p.pushToBuffer()
	err := p.createESClient()
	p.createIndex()
	_ = err
	for {
		msg := <-p.buffer
		err := p.indexDoc(msg)
		if err != nil {
			log.Printf("[EE] Failed to index msg: %s || %v", msg, err)
		}
	}
}
