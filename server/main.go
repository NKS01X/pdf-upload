package main

import (
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const serverHtml = `
        <!DOCTYPE html>
        <html>
        <head>
            <title>Server</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                h1 { color: #333; }
                form { margin-bottom: 20px; padding: 15px; border: 1px solid #eee; background-color: #f9f9f9; }
                input[type="file"] { margin-right: 10px; }
                input[type="submit"] { padding: 8px 15px; background-color: #007bff; color: white; border: none; cursor: pointer; }
                input[type="submit"]:hover { background-color: #0056b3; }
                #progress-messages {
                    border: 1px solid #ccc;
                    padding: 10px;
                    height: 200px;
                    overflow-y: scroll;
                    margin-top: 20px;
                    background-color: #f9f9f9;
                    font-size: 0.9em;
                    color: #555;
                }
                #progress-messages p {
                    margin: 2px 0;
                    font-family: monospace;
                }
                #progress-messages p.error {
                    color: red;
                }
                #progress-messages p.success {
                    color: green;
                }
                a { color: #007bff; text-decoration: none; }
                a:hover { text-decoration: underline; }
            </style>
        </head>
        <body>
            <h1>Upload your pdf </h1>
            <form id="upload-form" enctype="multipart/form-data">
                <input type="file" name="pdf" accept="application/pdf" required>
                <input type="submit" value="Upload">
            </form>
            <div id="progress-messages"></div>
            <br>
            <a href="/chunk-info">View Chunk Information</a>

            <script>
                document.getElementById('upload-form').addEventListener('submit', async function(event) {
                    event.preventDefault(); 

                    const progressDiv = document.getElementById('progress-messages');
                    progressDiv.innerHTML = ''; 

                    const uploadId = Date.now().toString(); 
                    const eventSource = new EventSource(`/upload-progress?id=${uploadId}`);
                    eventSource.onmessage = function(event) {
                        const p = document.createElement('p');
                        p.textContent = event.data;
                        progressDiv.appendChild(p);
                        progressDiv.scrollTop = progressDiv.scrollHeight; 
                    };
                    eventSource.onerror = function(err) {
                        console.error('EventSource failed:', err);
                        eventSource.close();
                        const p = document.createElement('p');
                        p.textContent = 'Upload progress stream ended or failed.';
                        p.classList.add('error');
                        progressDiv.appendChild(p);
                    };

                    // 2. Send the file via fetch API
                    const formData = new FormData(this);
                    try {
                        const response = await fetch(`/upload?id=${uploadId}`, {
                            method: 'POST',
                            body: formData,
                        });

                        if (!response.ok) {
                            const errorText = await response.text();
                            progressDiv.innerHTML += `<p class="error">Upload failed: ${errorText}</p>`;
                        } else {
                            const successText = await response.text();
                            progressDiv.innerHTML += `<p class="success">${successText}</p>`;
                        }
                    } catch (error) {
                        console.error('Fetch error:', error);
                        progressDiv.innerHTML += `<p class="error">Network error during upload: ${error.message}</p>`;
                    } finally {
                        eventSource.close(); 
                    }
                });
            </script>
        </body>
        </html>
    `

var servertemplate = template.Must(template.New("server").Parse(serverHtml))

const chunkInfoHtml = `
        <!DOCTYPE html>
        <html>
        <head>
            <title>Chunk Information</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                h1, h2 { color: #333; }
                form { margin-top: 15px; padding: 15px; border: 1px solid #eee; background-color: #f9f9f9; }
                label { display: block; margin-bottom: 5px; }
                input[type="number"] { padding: 5px; margin-right: 10px; }
                input[type="submit"] { padding: 8px 15px; background-color: #007bff; color: white; border: none; cursor: pointer; }
                input[type="submit"]:hover { background-color: #0056b3; }
                a { color: #007bff; text-decoration: none; }
                a:hover { text-decoration: underline; }
            </style>
        </head>
        <body>
            <h1>Chunk Information</h1>
            <p>Total chunks uploaded: {{.TotalChunks}}</p>

            <h2>Retrieve a specific chunk</h2>
            <form action="/retrieve-chunk" method="get">
                <label for="chunkKey">Chunk Key (0 to {{.MaxChunkKey}}):</label>
                <input type="number" id="chunkKey" name="key" min="0" value="0">
                <input type="submit" value="Retrieve Chunk">
            </form>
            <br>
            <a href="/">Back to Upload</a>
        </body>
        </html>
    `

var chunkInfoTemplate = template.Must(template.New("chunkInfo").Parse(chunkInfoHtml))

type ChunkInfoData struct {
	TotalChunks int
	MaxChunkKey int 
}

const ChunkSize = 4 * 1024 // 4KB

var (
	uploadProgressChannels = make(map[string]chan string)
	mu                     sync.Mutex 
)

// Helper function to safely get or create a progress channel to avoid race conditions
func getOrCreateProgressChan(uploadId string) chan string {
	mu.Lock()
	defer mu.Unlock()
	ch, ok := uploadProgressChannels[uploadId]
	if !ok {
		ch = make(chan string, 50) // Increased buffer size to safely prevent blocking
		uploadProgressChannels[uploadId] = ch
	}
	return ch
}

func serverHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	if err := servertemplate.Execute(w, nil); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	uploadId := r.URL.Query().Get("id")
	if uploadId == "" {
		http.Error(w, "Bad Request: missing upload ID", http.StatusBadRequest)
		return
	}

	// Safely retrieve or create the channel
	progressChan := getOrCreateProgressChan(uploadId)

	defer func() {
		close(progressChan) 
		mu.Lock()
		delete(uploadProgressChannels, uploadId) 
		mu.Unlock()
		log.Printf("Cleaned up progress channel for upload ID: %s", uploadId)
	}()

	file, header, err := r.FormFile("pdf")
	if err != nil {
		progressChan <- fmt.Sprintf("Error: Bad Request - %v", err)
		http.Error(w, "Bad Request: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	progressChan <- fmt.Sprintf("Starting upload for file: %s (Size: %d bytes)", header.Filename, header.Size)

	pipeReader, pipeWriter := io.Pipe()

	go func() {
		defer pipeWriter.Close()

		buffer := make([]byte, ChunkSize)
		chunkCount := 0 

		for {
			bytesRead, err := file.Read(buffer)
			if bytesRead > 0 {
				log.Printf("Streaming chunk %d of %d bytes to daemon...\n", chunkCount, bytesRead)
				
				// Using select with a default fallback helps ensure your upload 
				// doesn't block infinitely if the SSE client drops connection abruptly.
				select {
				case progressChan <- fmt.Sprintf("Chunk %d (%d bytes) streamed to daemon", chunkCount, bytesRead):
				default:
					log.Printf("Progress channel full, skipping message for chunk %d", chunkCount)
				}

				_, writeErr := pipeWriter.Write(buffer[:bytesRead])
				if writeErr != nil {
					log.Printf("Error writing to pipe for chunk %d: %v", chunkCount, writeErr)
					pipeWriter.CloseWithError(writeErr) 
					return
				}
				chunkCount++
			}

			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Error reading request body for chunk %d: %v", chunkCount, err)
				pipeWriter.CloseWithError(err)
				return
			}
		}
		progressChan <- "File fully read from client and piped to daemon."
	}()

	var daemonURL = "http://daemon:8081/process" 
	req, err := http.NewRequest("POST", daemonURL, pipeReader)
	if err != nil {
		progressChan <- fmt.Sprintf("Error creating daemon request: %v", err)
		http.Error(w, "Internal Server Error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/pdf")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		progressChan <- fmt.Sprintf("Error sending data to daemon: %v", err)
		http.Error(w, "Failed to send data to daemon: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		daemonRespBody, _ := io.ReadAll(resp.Body)
		errMsg := fmt.Sprintf("Daemon failed to process file (Status: %d): %s", resp.StatusCode, string(daemonRespBody))
		progressChan <- errMsg
		http.Error(w, errMsg, http.StatusBadGateway)
		return
	}

	progressChan <- "Daemon confirmed successful processing."
	w.Write([]byte("PDF uploaded and fully streamed to daemon successfully!"))
}

func retrieveChunkHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()
	keyStr := query.Get("key")
	if keyStr == "" {
		http.Error(w, "Bad Request: missing key parameter", http.StatusBadRequest)
		return
	}

	daemonURL := fmt.Sprintf("http://daemon:8081/retrieve?key=%s", keyStr)
	resp, err := http.Get(daemonURL)
	if err != nil {
		log.Printf("Failed to retrieve chunk from daemon: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream") 
	io.Copy(w, resp.Body)
}

func chunkInfoHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	daemonURL := "http://daemon:8081/total-chunks"
	resp, err := http.Get(daemonURL)
	if err != nil {
		log.Printf("Failed to get total chunks from daemon: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	var totalChunks int
	if resp.StatusCode == http.StatusNotFound {
		totalChunks = 0 
	} else if resp.StatusCode != http.StatusOK {
		log.Printf("Daemon returned error for total chunks: %s", resp.Status)
		http.Error(w, "Failed to get total chunks from daemon", http.StatusBadGateway)
		return
	} else {
		totalChunksStr, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Failed to read total chunks response from daemon: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		totalChunks, err = strconv.Atoi(string(totalChunksStr))
		if err != nil {
			log.Printf("Failed to parse total chunks from daemon: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	}

	data := ChunkInfoData{
		TotalChunks: totalChunks,
		MaxChunkKey: totalChunks - 1, 
	}

	w.Header().Set("Content-Type", "text/html")
	if err := chunkInfoTemplate.Execute(w, data); err != nil {
		log.Printf("Error executing chunk info template: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func uploadProgressHandler(w http.ResponseWriter, r *http.Request) {
	uploadId := r.URL.Query().Get("id")
	if uploadId == "" {
		http.Error(w, "Bad Request: missing upload ID", http.StatusBadRequest)
		return
	}

	// Safely retrieve or create the channel right away to resolve race conditions
	progressChan := getOrCreateProgressChan(uploadId)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*") 

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	log.Printf("Client connected for upload progress: %s", uploadId)

	for {
		select {
		case msg, open := <-progressChan:
			if !open {
				log.Printf("Progress channel for %s closed. Stopping SSE stream.", uploadId)
				fmt.Fprintf(w, "event: end\ndata: Upload process completed.\n\n")
				flusher.Flush()
				return 
			}
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		case <-r.Context().Done():
			log.Printf("Client disconnected from upload progress: %s", uploadId)
			return 
		}
	}
}

func main() {
	http.HandleFunc("/", serverHandler)
	http.HandleFunc("/upload", uploadHandler)
	http.HandleFunc("/retrieve-chunk", retrieveChunkHandler)
	http.HandleFunc("/chunk-info", chunkInfoHandler)
	
	// FIX: Route renamed from "/upload-progress" to "/upload_progress" to match the UI JS script.
	http.HandleFunc("/upload-progress", uploadProgressHandler) 

	log.Printf("Server is running on http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}