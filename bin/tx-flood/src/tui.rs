use std::{collections::VecDeque, io, thread, time::{Duration, Instant}};

use alloy::primitives::U256;
use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use otc_models::ChainType;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState, Wrap},
    Frame, Terminal,
};
use tokio::sync::{
    mpsc::{error::TryRecvError, UnboundedReceiver},
    oneshot,
};
use uuid::Uuid;

use crate::status::{SwapStage, SwapUpdate, UiEvent};

pub type TuiHandle = thread::JoinHandle<Result<()>>;

pub fn spawn_tui(
    receiver: UnboundedReceiver<UiEvent>,
    total_swaps: usize,
    deposit_chain: ChainType,
) -> (TuiHandle, oneshot::Receiver<()>) {
    let (exit_tx, exit_rx) = oneshot::channel();
    let handle = thread::spawn(move || run(receiver, total_swaps, deposit_chain, exit_tx));
    (handle, exit_rx)
}

fn run(
    mut receiver: UnboundedReceiver<UiEvent>,
    total_swaps: usize,
    deposit_chain: ChainType,
    exit_tx: oneshot::Sender<()>,
) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(total_swaps, deposit_chain, exit_tx);
    let mut last_draw = Instant::now();
    let draw_interval = Duration::from_millis(16); // 60 FPS target

    loop {
        // Priority 1: Handle keyboard events first (1ms timeout for responsiveness)
        if event::poll(Duration::from_millis(1))? {
            if let Event::Key(key_event) = event::read()? {
                app.handle_key_event(key_event);
                // Immediate redraw for user input
                terminal.draw(|f| app.draw(f))?;
                last_draw = Instant::now();
                
                if app.should_exit() {
                    break;
                }
                continue;
            }
        }

        // Priority 2: Process UI events in batches
        let mut ui_updates = false;
        for _ in 0..100 {
            match receiver.try_recv() {
                Ok(event) => {
                    app.handle_event(event);
                    ui_updates = true;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    app.mark_channel_closed();
                    break;
                }
            }
        }

        // Priority 3: Throttled redraw for UI updates
        if ui_updates && last_draw.elapsed() >= draw_interval {
            terminal.draw(|f| app.draw(f))?;
            last_draw = Instant::now();
        }

        if app.should_exit() {
            break;
        }
    }

    disable_raw_mode()?;
    crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

struct App {
    rows: Vec<SwapRow>,
    total: usize,
    deposit_chain: ChainType,
    shutdown: bool,
    receiver_closed: bool,
    exit_notifier: Option<oneshot::Sender<()>>,
    logs: VecDeque<String>,
    view_mode: ViewMode,
    table_scroll: TableScroll,
    log_scroll: LogScroll,
    count_buffer: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ViewMode {
    Dashboard,
    Logs,
}

#[derive(Default)]
struct TableScroll {
    state: TableState,
}

impl TableScroll {
    fn ensure_in_bounds(&mut self, len: usize) {
        if len == 0 {
            self.state.select(None);
            return;
        }
        match self.state.selected() {
            Some(idx) if idx < len => {}
            _ => self.state.select(Some(len - 1)),
        }
    }

    fn scroll_up(&mut self, lines: usize, len: usize) {
        if len == 0 {
            self.state.select(None);
            return;
        }
        let current = self.state.selected().unwrap_or(len - 1);
        let new_index = current.saturating_sub(lines);
        self.state.select(Some(new_index));
    }

    fn scroll_down(&mut self, lines: usize, len: usize) {
        if len == 0 {
            self.state.select(None);
            return;
        }
        let current = self.state.selected().unwrap_or(0);
        let new_index = std::cmp::min(current.saturating_add(lines), len - 1);
        self.state.select(Some(new_index));
    }

    fn scroll_to_start(&mut self, len: usize) {
        if len == 0 {
            self.state.select(None);
        } else {
            self.state.select(Some(0));
        }
    }

    fn snap_to_end(&mut self, len: usize) {
        if len == 0 {
            self.state.select(None);
        } else {
            self.state.select(Some(len - 1));
        }
    }

    fn is_at_end(&self, len: usize) -> bool {
        if len == 0 {
            true
        } else {
            match self.state.selected() {
                Some(idx) => idx >= len.saturating_sub(1),
                None => true,
            }
        }
    }
}

#[derive(Default)]
struct LogScroll {
    offset: usize,
}

impl LogScroll {
    fn scroll_up(&mut self, lines: usize, _len: usize) {
        self.offset = self.offset.saturating_sub(lines);
    }

    fn scroll_down(&mut self, lines: usize, len: usize) {
        if len == 0 {
            self.offset = 0;
        } else {
            let max_offset = len.saturating_sub(1);
            self.offset = std::cmp::min(self.offset.saturating_add(lines), max_offset);
        }
    }

    fn scroll_to_start(&mut self) {
        self.offset = 0;
    }

    fn snap_to_end(&mut self, len: usize) {
        if len == 0 {
            self.offset = 0;
        } else {
            self.offset = len - 1;
        }
    }

    fn ensure_in_bounds(&mut self, len: usize) {
        if len == 0 {
            self.offset = 0;
        } else {
            let max_offset = len.saturating_sub(1);
            if self.offset > max_offset {
                self.offset = max_offset;
            }
        }
    }

    fn is_at_end(&self, len: usize) -> bool {
        len == 0 || self.offset >= len.saturating_sub(1)
    }

    fn on_pop_front(&mut self) {
        if self.offset > 0 {
            self.offset -= 1;
        }
    }

    fn view_start(&self, len: usize, view_height: usize) -> usize {
        if len == 0 || view_height == 0 {
            0
        } else {
            let max_start = len.saturating_sub(view_height);
            std::cmp::min(self.offset, max_start)
        }
    }
}

impl App {
    fn new(total: usize, deposit_chain: ChainType, exit_notifier: oneshot::Sender<()>) -> Self {
        let rows = (0..total).map(SwapRow::new).collect();
        let mut app = Self {
            rows,
            total,
            deposit_chain,
            shutdown: false,
            receiver_closed: false,
            exit_notifier: Some(exit_notifier),
            logs: VecDeque::new(),
            view_mode: ViewMode::Dashboard,
            table_scroll: TableScroll::default(),
            log_scroll: LogScroll::default(),
            count_buffer: String::new(),
        };
        app.table_scroll.snap_to_end(app.rows.len());
        app
    }

    fn handle_event(&mut self, event: UiEvent) {
        match event {
            UiEvent::Swap(update) => self.apply_swap_update(update),
            UiEvent::Log(line) => self.push_log(line),
        }
    }

    fn apply_swap_update(&mut self, update: SwapUpdate) {
        if matches!(update.stage, SwapStage::Shutdown) {
            self.shutdown = true;
            self.notify_exit();
            return;
        }
        if update.index >= self.rows.len() {
            return;
        }
        let follow = self.table_scroll.is_at_end(self.rows.len());
        if let Some(row) = self.rows.get_mut(update.index) {
            row.apply(update);
        }
        if follow {
            self.table_scroll.snap_to_end(self.rows.len());
        } else {
            self.table_scroll.ensure_in_bounds(self.rows.len());
        }
    }

    fn mark_channel_closed(&mut self) {
        self.receiver_closed = true;
    }

    fn push_log(&mut self, line: String) {
        const MAX_LOG_LINES: usize = 500;
        let was_at_end = self.log_scroll.is_at_end(self.logs.len());
        if self.logs.len() >= MAX_LOG_LINES {
            self.logs.pop_front();
            self.log_scroll.on_pop_front();
        }
        self.logs.push_back(line);
        if was_at_end {
            self.log_scroll.snap_to_end(self.logs.len());
        } else {
            self.log_scroll.ensure_in_bounds(self.logs.len());
        }
    }

    fn should_exit(&self) -> bool {
        self.shutdown
    }

    fn handle_key_event(&mut self, key: KeyEvent) {
        if key.kind != KeyEventKind::Press {
            return;
        }
        let ctrl_c = key.modifiers.contains(KeyModifiers::CONTROL)
            && matches!(key.code, KeyCode::Char('c' | 'C'));
        let quit_key = matches!(key.code, KeyCode::Char('q' | 'Q') | KeyCode::Esc);

        if ctrl_c || quit_key {
            self.shutdown = true;
            self.notify_exit();
            return;
        }

        match key.code {
            KeyCode::Tab => {
                self.count_buffer.clear();
                self.toggle_view_mode();
            }
            KeyCode::Char('l') | KeyCode::Char('L') => {
                self.count_buffer.clear();
                self.set_view_mode(ViewMode::Logs);
            }
            KeyCode::Char('d') | KeyCode::Char('D') => {
                self.count_buffer.clear();
                self.set_view_mode(ViewMode::Dashboard);
            }
            KeyCode::Char(ch @ '0'..='9') => {
                // Accumulate digits for count prefix (vim-style)
                self.count_buffer.push(ch);
            }
            KeyCode::Up | KeyCode::Char('k') => {
                let count = self.parse_and_clear_count();
                self.scroll_up(count);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let count = self.parse_and_clear_count();
                self.scroll_down(count);
            }
            KeyCode::PageUp | KeyCode::Char('b')
                if key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                self.count_buffer.clear();
                self.scroll_up(10);
            }
            KeyCode::PageDown | KeyCode::Char('f')
                if key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                self.count_buffer.clear();
                self.scroll_down(10);
            }
            KeyCode::Home => {
                self.count_buffer.clear();
                self.scroll_to_start();
            }
            KeyCode::End => {
                self.count_buffer.clear();
                self.scroll_to_end();
            }
            KeyCode::Char('g') => {
                // Support vim-style 'gg' for top
                if self.count_buffer == "g" {
                    self.count_buffer.clear();
                    self.scroll_to_start();
                } else {
                    self.count_buffer.clear();
                    self.count_buffer.push('g');
                }
            }
            KeyCode::Char('G') => {
                // Support vim-style 'G' for bottom, or with count for specific line
                let count = self.parse_and_clear_count();
                if count > 1 {
                    // Go to specific line (0-indexed internally)
                    self.scroll_to_line(count.saturating_sub(1));
                } else {
                    self.scroll_to_end();
                }
            }
            _ => {
                // Clear count buffer on any other key
                self.count_buffer.clear();
            }
        }
    }

    fn parse_and_clear_count(&mut self) -> usize {
        let count = if self.count_buffer.is_empty() {
            1
        } else {
            self.count_buffer.parse::<usize>().unwrap_or(1)
        };
        self.count_buffer.clear();
        count
    }

    fn scroll_to_line(&mut self, line: usize) {
        match self.view_mode {
            ViewMode::Dashboard => {
                let len = self.rows.len();
                if len > 0 {
                    let target = std::cmp::min(line, len - 1);
                    self.table_scroll.state.select(Some(target));
                }
            }
            ViewMode::Logs => {
                let len = self.logs.len();
                if len > 0 {
                    self.log_scroll.offset = std::cmp::min(line, len - 1);
                }
            }
        }
    }

    fn set_view_mode(&mut self, mode: ViewMode) {
        self.view_mode = mode;
        match self.view_mode {
            ViewMode::Dashboard => self.table_scroll.ensure_in_bounds(self.rows.len()),
            ViewMode::Logs => self.log_scroll.ensure_in_bounds(self.logs.len()),
        }
    }

    fn toggle_view_mode(&mut self) {
        let next = match self.view_mode {
            ViewMode::Dashboard => ViewMode::Logs,
            ViewMode::Logs => ViewMode::Dashboard,
        };
        self.set_view_mode(next);
    }

    fn scroll_up(&mut self, lines: usize) {
        match self.view_mode {
            ViewMode::Dashboard => self.table_scroll.scroll_up(lines, self.rows.len()),
            ViewMode::Logs => self.log_scroll.scroll_up(lines, self.logs.len()),
        }
    }

    fn scroll_down(&mut self, lines: usize) {
        match self.view_mode {
            ViewMode::Dashboard => self.table_scroll.scroll_down(lines, self.rows.len()),
            ViewMode::Logs => self.log_scroll.scroll_down(lines, self.logs.len()),
        }
    }

    fn scroll_to_start(&mut self) {
        match self.view_mode {
            ViewMode::Dashboard => self.table_scroll.scroll_to_start(self.rows.len()),
            ViewMode::Logs => self.log_scroll.scroll_to_start(),
        }
    }

    fn scroll_to_end(&mut self) {
        match self.view_mode {
            ViewMode::Dashboard => self.table_scroll.snap_to_end(self.rows.len()),
            ViewMode::Logs => self.log_scroll.snap_to_end(self.logs.len()),
        }
    }

    fn notify_exit(&mut self) {
        if let Some(tx) = self.exit_notifier.take() {
            let _ = tx.send(());
        }
    }

    fn completed_count(&self) -> usize {
        self.rows.iter().filter(|row| row.is_terminal).count()
    }

    fn get_visible_rows(&self, viewport_height: usize) -> (usize, Vec<&SwapRow>) {
        if self.rows.is_empty() {
            return (0, Vec::new());
        }

        let buffer = 10;
        let selected = self.table_scroll.state.selected().unwrap_or(0);
        
        // Calculate visible window centered around selection with buffer
        let half_viewport = viewport_height / 2;
        let start = selected.saturating_sub(half_viewport + buffer);
        let end = (start + viewport_height + buffer * 2).min(self.rows.len());
        
        let visible_rows: Vec<&SwapRow> = self.rows[start..end].iter().collect();
        (start, visible_rows)
    }

    fn draw(&mut self, frame: &mut Frame<'_>) {
        match self.view_mode {
            ViewMode::Dashboard => self.draw_dashboard(frame),
            ViewMode::Logs => self.draw_logs(frame),
        }
    }

    fn draw_dashboard(&mut self, frame: &mut Frame<'_>) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(5),
                Constraint::Length(1),
            ])
            .split(frame.area());

        let count_hint = if self.count_buffer.is_empty() {
            String::new()
        } else {
            format!(" | Count: {}", self.count_buffer)
        };
        let header = format!(
            "Completed {}/{} | Deposit: {:?}{} | View: Dashboard (j/k scroll, g/G top/bottom, Tab toggles)",
            self.completed_count(),
            self.total,
            self.deposit_chain,
            count_hint
        );
        let header_block = Block::default()
            .title(header)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Blue));
        frame.render_widget(header_block, layout[0]);

        self.table_scroll.ensure_in_bounds(self.rows.len());

        // Virtual scrolling: only render visible rows
        let table_area = layout[1];
        let viewport_height = table_area.height.saturating_sub(3) as usize; // Subtract borders and header
        let (offset, visible_rows) = self.get_visible_rows(viewport_height);

        let rows = visible_rows.iter().map(|row| row.to_table_row());
        let table = Table::new(
            rows,
            [
                Constraint::Length(8),
                Constraint::Length(12),
                Constraint::Length(8),
                Constraint::Length(12),
                Constraint::Length(8),
                Constraint::Length(28),
                Constraint::Min(25),
                Constraint::Length(18),
                Constraint::Length(10),
            ],
        )
        .header(
            Row::new([
                Cell::from("#"),
                Cell::from("Swap"),
                Cell::from("Deposit"),
                Cell::from("Amount"),
                Cell::from("Sender"),
                Cell::from("Stage"),
                Cell::from("Detail"),
                Cell::from("Tx Hash"),
                Cell::from("Updated"),
            ])
            .style(Style::default().add_modifier(Modifier::BOLD)),
        )
        .block(Block::default().borders(Borders::ALL))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED))
        .highlight_symbol("▶ ");

        // Adjust table state to account for offset
        let mut adjusted_state = self.table_scroll.state.clone();
        if let Some(selected) = adjusted_state.selected() {
            adjusted_state.select(Some(selected.saturating_sub(offset)));
        }
        
        frame.render_stateful_widget(table, table_area, &mut adjusted_state);

        let footer = if self.shutdown {
            "Shutting down..."
        } else if self.completed_count() >= self.total && self.receiver_closed {
            "Swaps complete. Press Ctrl+C or Q to exit"
        } else if self.completed_count() >= self.total {
            "Swaps complete"
        } else if self.receiver_closed {
            "No more updates"
        } else {
            "Running"
        };

        let footer_block = Block::default()
            .title(footer)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray));
        frame.render_widget(footer_block, layout[2]);
    }

    fn draw_logs(&mut self, frame: &mut Frame<'_>) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(1), Constraint::Length(1)])
            .split(frame.area());

        let log_area = layout[0];
        let inner_height = log_area.height.saturating_sub(2) as usize;
        let view_height = inner_height.max(1);
        let len = self.logs.len();
        self.log_scroll.ensure_in_bounds(len);
        let start = self.log_scroll.view_start(len, view_height);

        let mut log_lines: Vec<Line> = if len == 0 {
            vec![Line::from("No log messages yet")]
        } else {
            self.logs
                .iter()
                .skip(start)
                .take(view_height)
                .map(|line| Line::from(line.as_str()))
                .collect()
        };

        if log_lines.is_empty() {
            log_lines.push(Line::from(""));
        }

        let log_block = Paragraph::new(log_lines).wrap(Wrap { trim: false }).block(
            Block::default()
                .title("Logs (j/k scroll, g/G top/bottom, Tab toggles)")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        );
        frame.render_widget(log_block, layout[0]);

        let footer_text = if self.receiver_closed {
            "No more updates"
        } else {
            "Streaming logs"
        };

        let count_hint = if self.count_buffer.is_empty() {
            String::new()
        } else {
            format!(" | Count: {}", self.count_buffer)
        };

        let footer_line = Line::from(vec![Span::styled(
            format!("{} | View: Logs{}", footer_text, count_hint),
            Style::default().fg(Color::Gray),
        )]);

        let footer = Paragraph::new(footer_line).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        );

        frame.render_widget(footer, layout[1]);
    }
}

struct SwapRow {
    index: usize,
    swap_id: Option<Uuid>,
    status_text: String,
    detail_text: String,
    tx_hash: Option<String>,
    amount: Option<U256>,
    deposit_chain: Option<ChainType>,
    sender_address: Option<String>,
    last_update: Option<String>,
    is_terminal: bool,
    is_error: bool,
}

impl SwapRow {
    fn new(index: usize) -> Self {
        Self {
            index,
            swap_id: None,
            status_text: "pending".to_string(),
            detail_text: String::new(),
            tx_hash: None,
            amount: None,
            deposit_chain: None,
            sender_address: None,
            last_update: None,
            is_terminal: false,
            is_error: false,
        }
    }

    fn apply(&mut self, update: SwapUpdate) {
        if let Some(amount) = update.amount {
            self.amount = Some(amount);
        }
        if let Some(chain) = update.deposit_chain {
            self.deposit_chain = Some(chain);
        }
        if let Some(address) = update.sender_address {
            self.sender_address = Some(address);
        }
        self.last_update = Some(update.timestamp.format("%H:%M:%S").to_string());
        match update.stage {
            SwapStage::QuoteRequested => {
                self.status_text = "quote requested".to_string();
                self.detail_text.clear();
                self.is_error = false;
                self.is_terminal = false;
                self.tx_hash = None;
            }
            SwapStage::QuoteFailed { reason } => {
                self.status_text = "quote failed".to_string();
                self.detail_text = reason;
                self.is_terminal = true;
                self.is_error = true;
            }
            SwapStage::QuoteReceived { quote_id } => {
                self.status_text = "quote received".to_string();
                self.detail_text = format!("quote {}", short_uuid(&quote_id));
                self.is_terminal = false;
                self.is_error = false;
                self.tx_hash = None;
            }
            SwapStage::SwapSubmitted { swap_id } => {
                self.swap_id = Some(swap_id);
                self.status_text = "swap submitted".to_string();
                self.detail_text = short_uuid(&swap_id);
                self.is_terminal = false;
                self.is_error = false;
            }
            SwapStage::PaymentBroadcast { swap_id, tx_hash } => {
                self.swap_id = Some(swap_id);
                self.status_text = "payment broadcast".to_string();
                self.tx_hash = Some(short_hash(&tx_hash));
                self.detail_text = format!("tx {}", short_hash(&tx_hash));
                self.is_terminal = false;
                self.is_error = false;
            }
            SwapStage::PaymentFailed { swap_id, reason } => {
                if let Some(id) = swap_id {
                    self.swap_id = Some(id);
                }
                self.status_text = "payment failed".to_string();
                self.detail_text = reason;
                self.is_terminal = true;
                self.is_error = true;
            }
            SwapStage::StatusUpdated { swap_id, status } => {
                self.swap_id = Some(swap_id);
                self.status_text = status;
                self.is_error = false;
                self.is_terminal = false;
            }
            SwapStage::Settled { swap_id } => {
                self.swap_id = Some(swap_id);
                self.status_text = "Settled".to_string();
                self.detail_text.clear();
                self.is_terminal = true;
                self.is_error = false;
            }
            SwapStage::FinishedWithError { swap_id, reason } => {
                if let Some(id) = swap_id {
                    self.swap_id = Some(id);
                }
                self.status_text = "error".to_string();
                self.detail_text = reason;
                self.is_terminal = true;
                self.is_error = true;
            }
            SwapStage::Shutdown => {}
        }
    }

    fn to_table_row(&self) -> Row<'_> {
        let swap_cell = self
            .swap_id
            .map(|id| short_uuid(&id).to_string())
            .unwrap_or_else(|| "-".to_string());
        let deposit_cell = self
            .deposit_chain
            .map(|chain| match chain {
                ChainType::Bitcoin => "BTC".to_string(),
                ChainType::Ethereum => "ETH".to_string(),
                ChainType::Base => "BASE".to_string(),
            })
            .unwrap_or_else(|| "-".to_string());
        let amount_cell = self
            .amount
            .map(format_amount)
            .unwrap_or_else(|| "-".to_string());
        let sender_cell = self
            .sender_address
            .as_ref()
            .map(|addr| truncate_address(addr, 6))
            .unwrap_or_else(|| "-".to_string());
        let tx_cell = self.tx_hash.clone().unwrap_or_else(|| "-".to_string());
        let updated = self
            .last_update
            .clone()
            .unwrap_or_else(|| "--:--:--".to_string());

        let style = if self.is_terminal {
            if self.is_error {
                Style::default().fg(Color::Red)
            } else {
                Style::default().fg(Color::Green)
            }
        } else {
            Style::default()
        };

        Row::new(vec![
            Cell::from(self.index.to_string()),
            Cell::from(swap_cell),
            Cell::from(deposit_cell),
            Cell::from(amount_cell),
            Cell::from(sender_cell),
            Cell::from(self.status_text.clone()),
            Cell::from(self.detail_text.clone()),
            Cell::from(tx_cell),
            Cell::from(updated),
        ])
        .style(style)
    }
}

fn short_uuid(id: &Uuid) -> String {
    id.to_string()
        .split('-')
        .next()
        .unwrap_or_default()
        .to_string()
}

fn short_hash(hash: &str) -> String {
    if hash.len() <= 10 {
        hash.to_string()
    } else {
        let prefix = &hash[..6];
        let suffix = &hash[hash.len() - 4..];
        format!("{}...{}", prefix, suffix)
    }
}

fn truncate_address(address: &str, chars: usize) -> String {
    if address.len() <= chars {
        address.to_string()
    } else {
        address.chars().take(chars).collect()
    }
}

fn format_amount(amount: U256) -> String {
    // Format with thousands separators for readability
    let s = amount.to_string();
    let mut result = String::new();
    let mut count = 0;

    for ch in s.chars().rev() {
        if count > 0 && count % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
        count += 1;
    }

    result.chars().rev().collect()
}
