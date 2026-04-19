package mini_stream

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"text/template"
)

type ShardView struct {
	ID        int
	Count     int
	TopicName string
}

type TopicView struct {
	Name   string
	Shards []ShardView
}

// This is what we will pass to the HTML template
type DashboardData struct {
	Topics []TopicView
}

type WebService struct {
	Ingestor  *Ingestor
	Templates *template.Template
}

func NewWebService(ingestor *Ingestor) *WebService {
	return &WebService{Ingestor: ingestor}
}

// Handler for the main page
func (ws *WebService) ServeDashboard(w http.ResponseWriter, r *http.Request) {
	data := ws.GetDashboardData() // Your data fetching logic

	// 1. Create a buffer to hold the template output
	var buf bytes.Buffer

	// 2. Execute template into the buffer (not the ResponseWriter yet)
	err := ws.Templates.Execute(&buf, data)
	if err != nil {
		// If it fails here, we haven't written to w yet, so we can safely send 500
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 3. If we got here, template executed successfully.
	// Now we can set headers and write the buffer.
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	buf.WriteTo(w)
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

// Handler that returns ONLY the stats fragment
func (ws *WebService) GetStatsFragment(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	shardStr := r.URL.Query().Get("shard")
	shardID, _ := strconv.Atoi(shardStr)

	count, err := ws.Ingestor.GetShardStats(topic, shardID)
	if err != nil {
		fmt.Fprintf(w, "0") // Fallback
		return
	}

	// Return ONLY the raw number string.
	// The parent <div> already has the styling (font-mono, text-4xl).
	fmt.Fprintf(w, "%d", count)
}

func (ws *WebService) GetDashboardData() DashboardData {
	ws.Ingestor.mu.RLock()
	defer ws.Ingestor.mu.RUnlock()

	data := DashboardData{}
	for name, topic := range ws.Ingestor.topics {
		tView := TopicView{Name: name}
		for i := range topic.Shards {
			count := 0
			topic.Shards[i].OffsetIndex.Range(func(_, _ any) bool { count++; return true })

			// Populate the new TopicName field here
			tView.Shards = append(tView.Shards, ShardView{
				ID:        i,
				Count:     count,
				TopicName: name, // Assign the name here
			})
		}
		data.Topics = append(data.Topics, tView)
	}
	return data
}
