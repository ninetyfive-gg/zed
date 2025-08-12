use crate::{NinetyFive, NinetyFiveCompletionStateId};
use anyhow::Result;
use edit_prediction::{Direction, EditPrediction, EditPredictionProvider};
use futures::StreamExt as _;
use gpui::{App, Context, Entity, EntityId, Task};
use language::{Anchor, Buffer, BufferSnapshot, EditPreview, TextDimension};
use project::Project;
use std::{
    ops::{AddAssign, Range},
    path::Path,
    time::Duration,
};
use text::{ToOffset, ToPoint};
use unicode_segmentation::UnicodeSegmentation;

pub struct NinetyFiveCompletionProvider {
    ninetyfive: Entity<NinetyFive>,
    buffer_id: Option<EntityId>,
    completion_id: Option<NinetyFiveCompletionStateId>,
    file_extension: Option<String>,
    pending_refresh: Option<Task<Result<()>>>,
}

impl NinetyFiveCompletionProvider {
    pub fn new(ninetyfive: Entity<NinetyFive>, _cx: &App) -> Self {
        Self {
            ninetyfive,
            buffer_id: None,
            completion_id: None,
            file_extension: None,
            pending_refresh: None,
        }
    }
}

fn completion_from_diff(
    snapshot: BufferSnapshot,
    completion_text: &str,
    position: Anchor,
    delete_range: Range<Anchor>,
) -> EditPrediction {
    let buffer_text = snapshot
        .text_for_range(delete_range.clone())
        .collect::<String>();

    let mut edits: Vec<(Range<language::Anchor>, String)> = Vec::new();

    let completion_graphemes: Vec<&str> = completion_text.graphemes(true).collect();
    let buffer_graphemes: Vec<&str> = buffer_text.graphemes(true).collect();

    let mut offset = position.to_offset(&snapshot);

    let mut i = 0;
    let mut j = 0;
    while i < completion_graphemes.len() && j < buffer_graphemes.len() {
        // find the next instance of the buffer text in the completion text.
        let k = completion_graphemes[i..]
            .iter()
            .position(|c| *c == buffer_graphemes[j]);
        match k {
            Some(k) => {
                if k != 0 {
                    let offset = snapshot.anchor_after(offset);
                    // the range from the current position to item is an inlay.
                    let edit = (offset..offset, completion_graphemes[i..i + k].join(""));
                    edits.push(edit);
                }
                i += k + 1;
                j += 1;
                // offset.add_assign(buffer_graphemes[j - 1].len());
                AddAssign::add_assign(&mut offset, buffer_graphemes[j - 1].len())
            }
            None => {
                // there are no more matching completions, so drop the remaining
                // completion text as an inlay.
                break;
            }
        }
    }

    if j == buffer_graphemes.len() && i < completion_graphemes.len() {
        let offset = snapshot.anchor_after(offset);
        // there is leftover completion text, so drop it as an inlay.
        let edit_range = offset..offset;
        let edit_text = completion_graphemes[i..].join("");
        edits.push((edit_range, edit_text));
    }

    EditPrediction {
        id: None,
        edits,
        edit_preview: None,
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
        false
    }

    fn is_enabled(&self, _buffer: &Entity<Buffer>, _cursor_position: Anchor, cx: &App) -> bool {
        self.ninetyfive.read(cx).is_enabled()
    }

    fn is_refreshing(&self) -> bool {
        false
    }

    fn refresh(
        &mut self,
        _project: Option<Entity<Project>>,
        buffer_handle: Entity<Buffer>,
        cursor_position: Anchor,
        debounce: bool,
        cx: &mut Context<Self>,
    ) {
        log::info!("NinetyFive: Refresh called (debounce: {})", debounce);
        let Some(mut completion) = self.ninetyfive.update(cx, |ninetyfive, cx| {
            ninetyfive.complete(&buffer_handle, cursor_position, cx)
        }) else {
            return;
        };

        self.pending_refresh = Some(cx.spawn(async move |this, cx| {
            if debounce {
                cx.background_executor()
                    .timer(Duration::from_millis(75))
                    .await;
            }

            while let Some(()) = completion.updates.next().await {
                this.update(cx, |this, cx| {
                    this.completion_id = Some(completion.id);
                    this.buffer_id = Some(buffer_handle.entity_id());
                    this.file_extension = buffer_handle.read(cx).file().and_then(|file| {
                        Some(
                            Path::new(file.file_name(cx))
                                .extension()?
                                .to_str()?
                                .to_string(),
                        )
                    });
                    this.pending_refresh = None;
                    cx.notify();
                })?;
            }
            Ok(())
        }));
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
        self.pending_refresh = None;
        self.completion_id = None;
    }

    fn discard(&mut self, _cx: &mut Context<Self>) {
        log::debug!("NinetyFive: Completion discarded");
        self.pending_refresh = None;
        self.completion_id = None;
    }

    fn suggest(
        &mut self,
        buffer: &Entity<Buffer>,
        cursor_position: language::Anchor,
        cx: &mut Context<Self>,
    ) -> Option<EditPrediction> {
        log::info!("NinetyFive: Suggest called");
        let completion_text = self
            .ninetyfive
            .read(cx)
            .completion(buffer, cursor_position, cx)?;

        let completion_text = trim_to_end_of_line_unless_leading_newline(completion_text);

        let completion_text = completion_text.trim_end();

        if !completion_text.trim().is_empty() {
            let snapshot = buffer.read(cx).snapshot();
            let mut point = cursor_position.to_point(&snapshot);
            point.column = snapshot.line_len(point.row);
            let range = cursor_position..snapshot.anchor_after(point);
            Some(completion_from_diff(
                snapshot,
                completion_text,
                cursor_position,
                range,
            ))
        } else {
            None
        }
    }
}

fn trim_to_end_of_line_unless_leading_newline(text: &str) -> &str {
    if has_leading_newline(text) {
        text
    } else if let Some(i) = text.find('\n') {
        &text[..i]
    } else {
        text
    }
}

fn has_leading_newline(text: &str) -> bool {
    for c in text.chars() {
        if c == '\n' {
            return true;
        }
        if !c.is_whitespace() {
            return false;
        }
    }
    false
}
