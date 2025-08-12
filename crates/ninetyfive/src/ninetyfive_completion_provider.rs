use crate::NinetyFive;
use anyhow::Result;
use async_tungstenite::{
    tokio::client_async_tls_with_connector_and_config,
    tungstenite::{protocol::WebSocketConfig, Message},
    WebSocketStream,
};
use edit_prediction::{Direction, EditPrediction, EditPredictionProvider};
use futures::{SinkExt, StreamExt};
use gpui::{App, Context, Entity, Task};
use gpui_tokio::Tokio;
use http_client_tls;
use language::{Anchor, Buffer, BufferSnapshot, EditPreview, ToOffset};
use project::Project;
use serde_json;
use std::{
    collections::HashMap,
    ops::Range,
    sync::{Arc, OnceLock},
};
use tokio::{net::TcpStream, sync::Mutex};

const NINETYFIVE_API_URL: &str = "wss://api.ninetyfive.gg";

type WebSocketConnection = WebSocketStream<async_tungstenite::tokio::ConnectStream>;

#[derive(Clone)]
struct CurrentCompletion {
    snapshot: BufferSnapshot,
    edits: Arc<[(Range<Anchor>, String)]>,
    edit_preview: EditPreview,
}

impl CurrentCompletion {
    fn interpolate(&self, new_snapshot: &BufferSnapshot) -> Option<Vec<(Range<Anchor>, String)>> {
        interpolate(&self.snapshot, new_snapshot, self.edits.clone())
    }
}

pub struct NinetyFiveCompletionProvider {
    ninetyfive: Entity<NinetyFive>,
    current_completion: Option<CurrentCompletion>,
}

static WEBSOCKET_CLIENT: OnceLock<Arc<WebSocketClient>> = OnceLock::new();

#[derive(Clone)]
pub struct WebSocketClient {
    api_url: String,
    connection: Arc<Mutex<Option<WebSocketConnection>>>,
}

impl WebSocketClient {
    fn new(api_url: String) -> Self {
        Self {
            api_url,
            connection: Arc::new(Mutex::new(None)),
        }
    }

    pub fn get_singleton(cx: &App) -> Arc<WebSocketClient> {
        WEBSOCKET_CLIENT
            .get_or_init(|| {
                let client = Arc::new(Self::new(NINETYFIVE_API_URL.to_string()));

                // Initialize connection in background
                let client_clone = client.clone();
                let task = Tokio::spawn(cx, async move {
                    if let Err(e) = client_clone.ensure_connection().await {
                        log::error!(
                            "NinetyFive: Failed to establish initial singleton connection: {}",
                            e
                        );
                    }
                });
                task.detach();

                client
            })
            .clone()
    }

    async fn create_connection(&self) -> Result<WebSocketConnection> {
        log::debug!(
            "NinetyFive: Creating websocket connection to {}",
            self.api_url
        );

        // Parse URL and connect to TCP stream first
        let url = url::Url::parse(&self.api_url)?;
        let host = url
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid host in URL"))?;
        let port = url.port().unwrap_or(443);

        log::debug!("NinetyFive: Connecting to TCP {}:{}", host, port);

        // Create TCP connection
        let tcp_stream = TcpStream::connect((host, port)).await?;
        log::debug!("NinetyFive: TCP connection successful");

        // Create websocket connection with TLS
        log::debug!(
            "NinetyFive: Attempting websocket handshake to {}",
            self.api_url
        );
        let (ws_stream, _) = client_async_tls_with_connector_and_config(
            &self.api_url,
            tcp_stream,
            Some(Arc::new(http_client_tls::tls_config()).into()),
            None,
        )
        .await?;

        log::info!("NinetyFive: Websocket connection established");
        Ok(ws_stream)
    }

    async fn ensure_connection(&self) -> Result<()> {
        let mut connection_guard = self.connection.lock().await;

        if connection_guard.is_none() {
            match self.create_connection().await {
                Ok(conn) => {
                    *connection_guard = Some(conn);
                    log::debug!("NinetyFive: Connection established and stored");
                }
                Err(e) => {
                    log::error!("NinetyFive: Failed to create connection: {}", e);
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    async fn reconnect_if_needed(&self) -> Result<()> {
        let mut connection_guard = self.connection.lock().await;

        // Always try to create a new connection
        match self.create_connection().await {
            Ok(conn) => {
                *connection_guard = Some(conn);
                log::debug!("NinetyFive: Reconnected successfully");
                Ok(())
            }
            Err(e) => {
                *connection_guard = None;
                log::error!("NinetyFive: Reconnection failed: {}", e);
                Err(e)
            }
        }
    }

    pub async fn send_file_content(&self, path: &str, content: &str) -> Result<()> {
        self.ensure_connection().await?;

        let message = serde_json::json!({
            "type": "file-content",
            "path": path,
            "text": content
        });

        let mut connection_guard = self.connection.lock().await;
        if let Some(ref mut ws_stream) = connection_guard.as_mut() {
            match ws_stream
                .send(Message::Text(message.to_string().into()))
                .await
            {
                Ok(_) => {
                    log::debug!("NinetyFive: Sent file content for {}", path);
                    Ok(())
                }
                Err(e) => {
                    log::error!("NinetyFive: Failed to send file content: {}", e);
                    // Drop the connection so it gets recreated next time
                    *connection_guard = None;
                    Err(e.into())
                }
            }
        } else {
            Err(anyhow::anyhow!("No websocket connection available"))
        }
    }

    pub async fn send_delta_completion_request(
        &self,
        pos: usize,
        repo: &str,
        file_path: Option<&str>,
        file_content: Option<&str>,
    ) -> Result<String> {
        self.ensure_connection().await?;

        // Always send file content before completion request if provided
        if let (Some(content)) = (file_content) {
            if let Err(e) = self.send_file_content("Untitled-1", content).await {
                log::warn!(
                    "NinetyFive: Failed to send file content before completion request: {}",
                    e
                );
                // Continue with completion request even if file content fails
                return Err(e);
            }
        } else {
            log::debug("NinetyFive: didnt send content");
            return Ok("".to_string());
        }

        let request_id = generate_request_id();
        let message = serde_json::json!({
            "type": "delta-completion-request",
            "requestId": request_id,
            "repo": repo,
            "pos": pos
        });

        let mut connection_guard = self.connection.lock().await;
        if let Some(ref mut ws_stream) = connection_guard.as_mut() {
            // Send the request
            match ws_stream
                .send(Message::Text(message.to_string().into()))
                .await
            {
                Ok(_) => {
                    log::debug!(
                        "NinetyFive: Sent delta completion request {} at pos {}",
                        request_id,
                        pos
                    );
                }
                Err(e) => {
                    log::error!("NinetyFive: Failed to send completion request: {}", e);
                    *connection_guard = None;
                    return Err(e.into());
                }
            }

            // Wait for response with timeout
            let timeout_duration = std::time::Duration::from_secs(10);
            let timeout_future = tokio::time::sleep(timeout_duration);
            tokio::pin!(timeout_future);

            let mut completion = String::new();

            loop {
                tokio::select! {
                    msg_result = ws_stream.next() => {
                        match msg_result {
                            Some(Ok(Message::Text(text))) => {
                                log::debug!("NinetyFive: Received websocket message: {}", text);

                                if let Ok(response) = serde_json::from_str::<serde_json::Value>(&text) {
                                    if let Some(response_id) = response.get("r").and_then(|r| r.as_str()) {
                                        if response_id == request_id {
                                            if let Some(value) = response.get("v").and_then(|v| v.as_str()) {
                                                completion.push_str(value);

                                                // Check if this is the final response
                                                if response.get("flush").and_then(|f| f.as_bool()).unwrap_or(true) {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Some(Ok(Message::Close(close_frame))) => {
                                log::info!("NinetyFive: Websocket connection closed by server: {:?}", close_frame);
                                *connection_guard = None;
                                return Err(anyhow::anyhow!("Connection closed by server"));
                            }
                            Some(Err(e)) => {
                                log::error!("NinetyFive: Websocket error: {}", e);
                                *connection_guard = None;
                                return Err(e.into());
                            }
                            None => {
                                log::debug!("NinetyFive: Websocket stream ended");
                                *connection_guard = None;
                                return Err(anyhow::anyhow!("Connection ended"));
                            }
                            _ => {
                                log::debug!("NinetyFive: Received non-text websocket message");
                            }
                        }
                    }
                    _ = &mut timeout_future => {
                        log::warn!("NinetyFive: Completion request {} timed out", request_id);
                        return Err(anyhow::anyhow!("Request timed out"));
                    }
                }
            }

            if completion.is_empty() {
                Ok("hello_ninetyfive_no_response".to_string())
            } else {
                Ok(completion)
            }
        } else {
            Err(anyhow::anyhow!("No websocket connection available"))
        }
    }
}

fn generate_request_id() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::{SystemTime, UNIX_EPOCH};

    let mut hasher = DefaultHasher::new();
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .hash(&mut hasher);
    format!("{:x}", hasher.finish())[..6].to_string()
}

impl NinetyFiveCompletionProvider {
    pub fn new(ninetyfive: Entity<NinetyFive>, _cx: &App) -> Self {
        Self {
            ninetyfive,
            current_completion: None,
        }
    }

    fn send_file_content_if_needed(&self, buffer: &Entity<Buffer>, cx: &App) {
        let client = WebSocketClient::get_singleton(cx);
        let buffer_snapshot = buffer.read(cx);
        if let Some(file) = buffer_snapshot.file() {
            let path = file.path().to_string_lossy().to_string();
            let content = buffer_snapshot.text();

            let task = Tokio::spawn(cx, async move {
                if let Err(e) = client.send_file_content(&path, &content).await {
                    log::error!("NinetyFive: Failed to send file content: {}", e);
                }
            });

            task.detach();
        }
    }

    async fn fetch_completion(
        &self,
        pos: usize,
        repo: &str,
        file_path: Option<&str>,
        file_content: Option<&str>,
        cx: &App,
    ) -> Result<String> {
        log::debug!("NinetyFive: Requesting completion at pos {}", pos);

        let client = WebSocketClient::get_singleton(cx);
        match client
            .send_delta_completion_request(pos, repo, file_path, file_content)
            .await
        {
            Ok(completion) => {
                log::debug!(
                    "NinetyFive: Received completion from websocket: '{}'",
                    completion
                );
                Ok(completion)
            }
            Err(err) => {
                log::error!("NinetyFive: Websocket request failed: {}", err);
                Ok("hello_ninetyfive_fallback".to_string())
            }
        }
    }
}

impl EditPredictionProvider for NinetyFiveCompletionProvider {
    fn name() -> &'static str {
        "ninetyfive"
    }

    fn display_name() -> &'static str {
        "NinetyFive"
    }

    fn show_completions_in_menu() -> bool {
        true
    }

    fn is_enabled(&self, _buffer: &Entity<Buffer>, _cursor_position: Anchor, cx: &App) -> bool {
        log::debug!("NinetyFive: is enabled enter");
        let enabled = self.ninetyfive.read(cx).is_enabled();
        log::debug!("NinetyFive: Provider enabled: {}", enabled);
        enabled
    }

    fn is_refreshing(&self) -> bool {
        false
    }

    fn refresh(
        &mut self,
        _project: Option<Entity<Project>>,
        _buffer_handle: Entity<Buffer>,
        _cursor_position: Anchor,
        debounce: bool,
        _cx: &mut Context<Self>,
    ) {
        log::debug!("NinetyFive: Refresh called (debounce: {})", debounce);
        self.current_completion = None;
    }

    fn cycle(
        &mut self,
        _buffer: Entity<Buffer>,
        _cursor_position: language::Anchor,
        _direction: Direction,
        _cx: &mut Context<Self>,
    ) {
        // Does nothing
    }

    fn accept(&mut self, _cx: &mut Context<Self>) {
        log::debug!("NinetyFive: Completion accepted");
        self.current_completion = None;
    }

    fn discard(&mut self, _cx: &mut Context<Self>) {
        log::debug!("NinetyFive: Completion discarded");
        self.current_completion = None;
    }

    fn suggest(
        &mut self,
        buffer: &Entity<Buffer>,
        cursor_position: language::Anchor,
        cx: &mut Context<Self>,
    ) -> Option<EditPrediction> {
        log::debug!("NinetyFive: Suggest called");

        // If we have a current completion, try to interpolate it
        if let Some(current_completion) = &self.current_completion {
            let buffer_snapshot = buffer.read(cx);
            if let Some(edits) = current_completion.interpolate(&buffer_snapshot.snapshot()) {
                if !edits.is_empty() {
                    return Some(EditPrediction {
                        id: None,
                        edits,
                        edit_preview: Some(current_completion.edit_preview.clone()),
                    });
                }
            }
        }

        // Get cursor position in bytes
        let buffer_snapshot = buffer.read(cx);
        let cursor_offset = cursor_position.to_offset(&buffer_snapshot);

        // Get repo name (fallback to "unknown")
        let repo = buffer_snapshot
            .file()
            .and_then(|file| {
                file.path()
                    .ancestors()
                    .find(|p| p.join(".git").exists())
                    .and_then(|p| p.file_name())
                    .map(|name| name.to_string_lossy().to_string())
            })
            .unwrap_or_else(|| "unknown".to_string());

        // Get file information for the completion request
        let (file_path, file_content) = if let Some(file) = buffer_snapshot.file() {
            let path = file.path().to_string_lossy().to_string();
            let content = buffer_snapshot.text();
            (Some(path), Some(content))
        } else {
            (None, None)
        };

        // Make async completion request using singleton connection
        let client = WebSocketClient::get_singleton(cx);
        let task = Tokio::spawn(cx, async move {
            match client
                .send_delta_completion_request(
                    cursor_offset,
                    &repo,
                    file_path.as_deref(),
                    file_content.as_deref(),
                )
                .await
            {
                Ok(completion) => {
                    log::debug!(
                        "NinetyFive: Received completion via singleton connection: '{}'",
                        completion
                    );
                    // TODO: Update the current_completion and trigger re-render
                }
                Err(e) => {
                    log::error!("NinetyFive: Completion request failed: {}", e);
                }
            }
        });

        task.detach();

        // Return placeholder for now
        let position = cursor_position.bias_right(&buffer_snapshot);
        Some(EditPrediction {
            id: None,
            edits: vec![(position..position, "hello_ws_friend".to_string())],
            edit_preview: None,
        })
    }
}

fn interpolate(
    old_snapshot: &BufferSnapshot,
    new_snapshot: &BufferSnapshot,
    current_edits: Arc<[(Range<Anchor>, String)]>,
) -> Option<Vec<(Range<Anchor>, String)>> {
    // We should only have one edit (cursor insertion) in the simplified model
    if current_edits.len() != 1 {
        return None;
    }

    let (edit_range, completion_text) = &current_edits[0];
    let cursor_offset = edit_range.start.to_offset(old_snapshot);

    // Check what the user has typed since the prediction
    for user_edit in new_snapshot.edits_since::<usize>(&old_snapshot.version) {
        // If the user edit is at our cursor position
        if user_edit.old.start == cursor_offset && user_edit.old.end == cursor_offset {
            let user_typed = new_snapshot
                .text_for_range(user_edit.new.clone())
                .collect::<String>();

            // Check if what the user typed matches the beginning of our completion
            if let Some(remaining) = completion_text.strip_prefix(&user_typed) {
                if remaining.is_empty() {
                    // User typed the entire completion
                    return None;
                }
                // Adjust to insert only the remaining part
                let new_cursor = new_snapshot.anchor_after(user_edit.new.end);
                return Some(vec![(new_cursor..new_cursor, remaining.to_string())]);
            } else if !user_typed.is_empty() {
                // User typed something different
                return None;
            }
        } else if user_edit.old.contains(&cursor_offset) || cursor_offset > user_edit.old.end {
            // User made an edit that affects our insertion point
            return None;
        }
    }

    // No conflicting edits, return original completion
    Some(vec![(edit_range.clone(), completion_text.clone())])
}
