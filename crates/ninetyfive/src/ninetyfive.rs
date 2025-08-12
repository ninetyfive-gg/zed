mod ninetyfive_completion_provider;

use gpui_tokio::Tokio;
pub use ninetyfive_completion_provider::*;

use anyhow::{Context as _, Result};
use async_tungstenite::tungstenite::client::IntoClientRequest;
use client::Client;
use collections::BTreeMap;
use futures::{SinkExt, StreamExt, channel::mpsc};
use gpui::{
    App, AppContext, AsyncApp, Context, Entity, EntityId, Global, Task, WeakEntity, actions,
};
use http_client::Request;
use language::{Anchor, Buffer, ToOffset, language_settings::all_language_settings};
use postage::watch;
use rand::{Rng, thread_rng};
use serde::{Deserialize, Serialize};
use settings::SettingsStore;
use std::{path::PathBuf, sync::Arc};

use tokio_tungstenite::{connect_async, tungstenite::Message};

actions!(ninetyfive, []);

pub fn init(client: Arc<Client>, cx: &mut App) {
    let ninetyfive = cx.new(|_| NinetyFive::Starting);
    NinetyFive::set_global(ninetyfive.clone(), cx);

    let mut provider = all_language_settings(None, cx).edit_predictions.provider;
    if provider == language::language_settings::EditPredictionProvider::NinetyFive {
        ninetyfive.update(cx, |ninetyfive, cx| ninetyfive.start(client.clone(), cx));
    }

    cx.observe_global::<SettingsStore>(move |cx| {
        let new_provider = all_language_settings(None, cx).edit_predictions.provider;
        if new_provider != provider {
            provider = new_provider;
            if provider == language::language_settings::EditPredictionProvider::NinetyFive {
                ninetyfive.update(cx, |ninetyfive, cx| ninetyfive.start(client.clone(), cx));
            } else {
                ninetyfive.update(cx, |ninetyfive, _cx| ninetyfive.stop());
            }
        }
    })
    .detach();
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
enum WebSocketMessage {
    #[serde(rename = "file-content")]
    FileContent { path: String, text: String },
    #[serde(rename = "delta-completion-request")]
    DeltaCompletionRequest {
        request_id: String,
        repo: String,
        pos: usize,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NinetyFiveMessage {
    SubscriptionInfo {
        is_paid: bool,
        name: String,
    },
    Response {
        r: String,
        v: String,
        end: Option<bool>,
        flush: Option<bool>,
    },
    #[serde(other)]
    Unknown,
}

pub enum NinetyFive {
    Starting,
    FailedConnection { error: anyhow::Error },
    Connected(NinetyFiveAgent),
    Error { error: anyhow::Error },
}

#[derive(Clone)]
struct NinetyFiveGlobal(Entity<NinetyFive>);

impl Global for NinetyFiveGlobal {}

impl NinetyFive {
    pub fn global(cx: &App) -> Option<Entity<Self>> {
        cx.try_global::<NinetyFiveGlobal>()
            .map(|model| model.0.clone())
    }

    pub fn set_global(ninetyfive: Entity<Self>, cx: &mut App) {
        cx.set_global(NinetyFiveGlobal(ninetyfive));
    }

    pub fn start(&mut self, client: Arc<Client>, cx: &mut Context<Self>) {
        if let Self::Starting = self {
            cx.spawn(async move |this, cx| {
                let ws_url = "wss://api.ninetyfive.gg";

                this.update(cx, |this, cx| {
                    if let Self::Starting = this {
                        *this = Self::Connected(NinetyFiveAgent::new(ws_url, client.clone(), cx)?);
                    }
                    anyhow::Ok(())
                })
            })
            .detach_and_log_err(cx);
        }
    }

    pub fn stop(&mut self) {
        *self = Self::Starting;
    }

    pub fn is_enabled(&self) -> bool {
        matches!(self, Self::Connected { .. })
    }

    // To be called when we want to request a suggestion to the server
    pub fn complete(
        &mut self,
        buffer: &Entity<Buffer>,
        cursor_position: Anchor,
        cx: &App,
    ) -> Option<NinetyFiveCompletion> {
        if let Self::Connected(agent) = self {
            let buffer_id = buffer.entity_id();
            let buffer = buffer.read(cx);
            let path = buffer
                .file()
                .and_then(|file| Some(file.as_local()?.abs_path(cx)))
                .unwrap_or_else(|| PathBuf::from("Untitled-1"))
                .to_string_lossy()
                .to_string();
            let content = buffer.text();
            let cursor_offset = cursor_position.to_offset(buffer);
            let state_id = agent.next_state_id;
            agent.next_state_id.0 += 1;

            let (updates_tx, mut updates_rx) = watch::channel();
            postage::stream::Stream::try_recv(&mut updates_rx).unwrap();

            agent.states.insert(
                state_id,
                NinetyFiveCompletionState {
                    buffer_id,
                    prefix_anchor: cursor_position,
                    prefix_offset: cursor_offset,
                    text: String::new(),
                    dedent: String::new(),
                    updates_tx,
                },
            );

            if agent.states.len() > 1000 {
                agent
                    .states
                    .remove(&agent.states.keys().next().unwrap().clone());
            }

            let request_id = generate_request_id();

            let _ = agent
                .outgoing_tx
                .unbounded_send(WebSocketMessage::FileContent {
                    path: path.clone(),
                    text: content,
                });

            let _ = agent
                .outgoing_tx
                .unbounded_send(WebSocketMessage::DeltaCompletionRequest {
                    request_id: request_id.clone(),
                    repo: "unknown".to_string(), //TODO(juaoose) change me
                    pos: cursor_offset,
                });

            Some(NinetyFiveCompletion {
                id: state_id,
                updates: updates_rx,
            })
        } else {
            None
        }
    }

    // To be called when we want to show a suggestion
    pub fn completion(
        &self,
        buffer: &Entity<Buffer>,
        cursor_position: Anchor,
        cx: &App,
    ) -> Option<&str> {
        if let Self::Connected(agent) = self {
            // wed find the completion here
            None
        } else {
            None
        }
    }
}

fn generate_request_id() -> String {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = thread_rng();

    (0..6)
        .map(|_| {
            let idx = rng.gen_range(0..CHARS.len());
            CHARS[idx] as char
        })
        .collect()
}

pub struct NinetyFiveAgent {
    next_state_id: NinetyFiveCompletionStateId,
    states: BTreeMap<NinetyFiveCompletionStateId, NinetyFiveCompletionState>,
    outgoing_tx: mpsc::UnboundedSender<WebSocketMessage>,
    _handle_outgoing_messages: Task<Result<()>>,
    _handle_incoming_messages: Task<Result<()>>,
    client: Arc<Client>,
    close_tx: Option<mpsc::UnboundedSender<()>>,
}

impl NinetyFiveAgent {
    fn new(ws_url: &str, client: Arc<Client>, cx: &mut Context<NinetyFive>) -> Result<Self> {
        let mut req: Request<()> = ws_url.into_client_request()?;
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded();
        let (close_tx, close_rx) = mpsc::unbounded();

        let handle_connection = cx.spawn({
            let client = client.clone();
            let outgoing_tx = outgoing_tx.clone();
            async move |this, cx| {
                let (ws_stream, _) = connect_async(req)
                    .await
                    .context("Failed to connect to NinetyFive server")?;

                let (mut ws_sink, mut ws_stream) = ws_stream.split();

                let mut status = client.status();
                while let Some(status) = status.next().await {
                    if status.is_connected() {
                        // this.update(cx, |this, cx| {
                        //     // TODO here we'd change account status
                        // })?;
                        break;
                    }
                }

                let outgoing_future = Self::handle_outgoing_messages(outgoing_rx, ws_sink);
                let incoming_future = Self::handle_incoming_messages(this, ws_stream, cx);

                // This will run both concurrently and return when both complete
                // (which should happen when the connection closes)
                let (outgoing_result, incoming_result) =
                    futures::future::join(outgoing_future, incoming_future).await;

                // Return the first error if any, otherwise Ok
                outgoing_result.and(incoming_result)
            }
        });

        Ok(Self {
            next_state_id: NinetyFiveCompletionStateId::default(),
            states: BTreeMap::default(),
            outgoing_tx,
            _handle_outgoing_messages: handle_connection,
            _handle_incoming_messages: Task::ready(Ok(())),
            client,
            close_tx: Some(close_tx),
        })
    }

    // Only in charge of sending messages to the server
    async fn handle_outgoing_messages(
        mut outgoing: mpsc::UnboundedReceiver<WebSocketMessage>,
        mut ws_sink: futures::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
            Message,
        >,
    ) -> Result<()> {
        while let Some(message) = outgoing.next().await {
            let json = serde_json::to_string(&message)?;
            ws_sink.send(Message::text(json)).await?;
        }
        Ok(())
    }

    async fn handle_incoming_messages(
        this: WeakEntity<NinetyFive>,
        mut ws_stream: futures::stream::SplitStream<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        >,
        cx: &mut AsyncApp,
    ) -> Result<()> {
        while let Some(msg) = ws_stream.next().await {
            let msg = msg.context("WebSocket error")?;

            match msg {
                Message::Text(text) => {
                    let message = serde_json::from_str::<NinetyFiveMessage>(&text)
                        .with_context(|| format!("Failed to deserialize message: {:?}", text));

                    match message {
                        Ok(message) => {
                            this.update(cx, |this, _cx| {
                                if let NinetyFive::Connected(this) = this {
                                    this.handle_message(message);
                                }
                                Task::ready(anyhow::Ok(()))
                            })?
                            .await?;
                        }
                        Err(e) => {
                            log::warn!("Failed to deserialize message: {}", e);
                        }
                    }
                }
                Message::Close(_) => {
                    log::info!("WebSocket connection closed");
                    break;
                }
                Message::Ping(payload) => {
                    log::info!("Received ping: {:?}", payload);
                }
                Message::Pong(_) => {
                    log::info!("Received pong");
                }
                Message::Binary(_) => {
                    log::warn!("Received unexpected binary message");
                }
                Message::Frame(_) => {
                    log::warn!("Received raw frame message");
                }
            }
        }

        Ok(())
    }

    fn handle_message(&mut self, message: NinetyFiveMessage) {
        match message {
            NinetyFiveMessage::SubscriptionInfo { .. } => {
                log::info!("Received sub info!");
            }
            NinetyFiveMessage::Response { r, v, .. } => {
                log::info!("received response {}, {}", r, v);
                // TODO this is pretty much crucial lmao
            }
            _ => {
                log::warn!("unhandled message: {:?}", message);
            }
        }
    }
}

impl Drop for NinetyFiveAgent {
    fn drop(&mut self) {
        if let Some(close_tx) = self.close_tx.take() {
            let _ = close_tx.unbounded_send(());
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct NinetyFiveCompletionStateId(usize);

#[allow(dead_code)]
pub struct NinetyFiveCompletionState {
    buffer_id: EntityId,
    prefix_anchor: Anchor,
    prefix_offset: usize,
    text: String,
    dedent: String,
    updates_tx: watch::Sender<()>,
}

pub struct NinetyFiveCompletion {
    pub id: NinetyFiveCompletionStateId,
    pub updates: watch::Receiver<()>,
}
