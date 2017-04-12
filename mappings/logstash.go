package mappings

import (
	"log"
	"encoding/json"
)


type Logstash struct {
	settings Settings "json:`settings`"
	mappings interface{} "json:`mappings`"
}
func NewLogstash(shards, replicas int) Logstash {
	return Logstash{
		settings: Settings{
			NumShards: 1,
			NumReplicas: 0,
		},
		mappings: map[string]interface{}{
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

}

func (l *Logstash) GetConfig() (interface{}, error) {
	marshalled, err := json.Marshal(l)
	if err != nil {
		log.Printf("[WW] Failed to marshall indexCfg: %v >> %v", l, err)
		return nil, err
	}
	return marshalled, nil
}
