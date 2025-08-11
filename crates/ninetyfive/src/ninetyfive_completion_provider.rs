use crate::NinetyFive;
use anyhow::Result;
use edit_prediction::{Direction, EditPrediction, EditPredictionProvider};
use gpui::{App, Context, Entity};
use language::{Anchor, Buffer, BufferSnapshot, EditPreview, ToOffset};
use project::Project;
use std::{ops::Range, sync::Arc};

const NINETYFIVE_API_URL: &str = "wss://api.ninetyfive.gg";

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

impl NinetyFiveCompletionProvider {
    pub fn new(ninetyfive: Entity<NinetyFive>) -> Self {
        Self {
            ninetyfive,
            current_completion: None,
        }
    }

    async fn fetch_completion(_input_excerpt: String, _outline: String) -> Result<String> {
        log::debug!("NinetyFive: Requesting completion");

        //TODO(juaoose): Actually send it to 95
        Ok("hello_ninetyfive".to_string())
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
        //TODO(juaoose): ??
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
        //Does nothing
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

        //TODO Just showwwwww it
        let buffer_snapshot = buffer.read(cx);
        let position = cursor_position.bias_right(&buffer_snapshot);
        Some(EditPrediction {
            id: None,
            edits: vec![(position..position, "hello friend".to_string())],
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
