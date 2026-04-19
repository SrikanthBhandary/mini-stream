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

func (ws *WebService) CreateTopic(w http.ResponseWriter, r *http.Request) {
	topicName := r.FormValue("topicName")
	shardStr := r.FormValue("shardCount")

	shardCount, err := strconv.Atoi(shardStr)
	if err != nil {
		shardCount = 1
	}

	// REMOVE THESE TWO LINES:
	// ws.Ingestor.mu.Lock()
	// ws.Ingestor.mu.Unlock()

	// Just call the method; it handles its own locking internally!
	err = ws.Ingestor.CreateTopic(topicName, shardCount)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (ws *WebService) GetMessagesModal(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	shardID, _ := strconv.Atoi(r.URL.Query().Get("shard"))

	fmt.Fprintf(w, `
<div class="modal-backdrop fixed inset-0 bg-black bg-opacity-80 flex items-center justify-center p-4">
    <div class="bg-gray-900 border border-gray-700 rounded-xl w-full max-w-2xl max-h-[80vh] flex flex-col">
        
        <div class="flex justify-between items-center p-6 border-b border-gray-800">
            <h2 class="text-xl font-bold">%s — Shard %d</h2>
            <button onclick="document.getElementById('modal-container').innerHTML=''"
                class="text-gray-500 hover:text-white text-2xl leading-none">&times;</button>
        </div>

        <div class="p-4 overflow-y-auto flex-1"
             hx-ext="sse"
             sse-connect="/api/shard/stream?topic=%s&shard=%d">
            <div id="message-list"
                 hx-swap="beforeend"
                 sse-swap="message"
                 class="space-y-2">
                <p class="text-gray-600 text-xs font-mono">Connecting to stream...</p>
            </div>
        </div>

    </div>
</div>`, topic, shardID, topic, shardID)
}

func (ws *WebService) StreamMessages(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	topicName := r.URL.Query().Get("topic")
	shardID, _ := strconv.Atoi(r.URL.Query().Get("shard"))

	topic, ok := ws.Ingestor.GetTopic(topicName)
	if !ok {
		http.Error(w, "topic not found", http.StatusNotFound)
		return
	}
	shard := topic.Shards[shardID]

	msgChan := shard.Subscribe()
	defer shard.Unsubscribe(msgChan)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Send a keepalive comment immediately so browser knows connection is open
	fmt.Fprintf(w, ": connected\n\n")
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-msgChan:
			// HTMX SSE sse-swap="beforeend" needs event: + data: format
			html := fmt.Sprintf(
				`<div class="p-2 bg-gray-800 rounded border border-gray-700 font-mono text-xs">%s</div>`,
				msg,
			)
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", html)
			flusher.Flush()
		}
	}
}
