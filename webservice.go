package mini_stream

import (
	"encoding/json"
	"net/http"
	"strconv"
)

type WebService struct {
	Ingestor *Ingestor
}

func NewWebService(ingestor *Ingestor) *WebService {
	return &WebService{Ingestor: ingestor}
}

func (ws *WebService) GetTopics(w http.ResponseWriter, r *http.Request) {
	topics := ws.Ingestor.ListTopics()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string][]string{"topics": topics})
}

func (ws *WebService) GetTotalMessagesInShard(w http.ResponseWriter, r *http.Request) {
	// Expect query params: ?topic=orders&shard=0
	topic := r.URL.Query().Get("topic")
	shardStr := r.URL.Query().Get("shard")
	shardID, _ := strconv.Atoi(shardStr)

	count, err := ws.Ingestor.GetShardStats(topic, shardID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"count": count})
}
