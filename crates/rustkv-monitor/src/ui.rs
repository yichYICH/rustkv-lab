use std::io;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Gauge, List, ListItem, Paragraph, Wrap};
use tokio::sync::watch;

use crate::app::{ConnectionStatus, MonitorState};

pub async fn run(mut rx: watch::Receiver<MonitorState>) -> Result<(), io::Error> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let result = run_loop(&mut terminal, &mut rx).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

async fn run_loop<B: Backend>(
    terminal: &mut Terminal<B>,
    rx: &mut watch::Receiver<MonitorState>,
) -> Result<(), io::Error> {
    let mut state = rx.borrow().clone();

    loop {
        if rx.has_changed().unwrap_or(false) {
            state = rx.borrow_and_update().clone();
        }

        terminal.draw(|frame| draw(frame, &state))?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press && key.code == KeyCode::Char('q') {
                    break;
                }
            }
        }
    }

    Ok(())
}

pub fn draw(frame: &mut Frame<'_>, state: &MonitorState) {
    let area = frame.size();
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Min(10),
            Constraint::Length(3),
        ])
        .split(area);

    draw_header(frame, vertical[0], state);

    let middle = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(vertical[1]);

    draw_metrics(frame, middle[0], state);
    draw_status(frame, middle[1], state);
    draw_help(frame, vertical[2]);
}

fn draw_header(frame: &mut Frame<'_>, area: Rect, state: &MonitorState) {
    let status_style = match state.status {
        ConnectionStatus::Online => Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD),
        ConnectionStatus::Connecting => Style::default().fg(Color::Yellow),
        ConnectionStatus::Offline => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
    };

    let reported_addr = if state.metrics.addr.is_empty() {
        state.addr.as_str()
    } else {
        state.metrics.addr.as_str()
    };

    let lines = vec![
        Line::from(vec![
            Span::styled(
                "rustkv monitor",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(state.status.label(), status_style),
        ]),
        Line::from(vec![
            Span::styled("Address: ", Style::default().fg(Color::DarkGray)),
            Span::raw(reported_addr),
            Span::raw("    "),
            Span::styled("Uptime: ", Style::default().fg(Color::DarkGray)),
            Span::raw(state.uptime_text()),
            Span::raw("    "),
            Span::styled("Updated: ", Style::default().fg(Color::DarkGray)),
            Span::raw(state.last_updated_text()),
        ]),
        Line::from(vec![
            Span::styled("Version: ", Style::default().fg(Color::DarkGray)),
            Span::raw(default_text(&state.metrics.server_version, "unknown")),
            Span::raw("    "),
            Span::styled("Role: ", Style::default().fg(Color::DarkGray)),
            Span::raw(default_text(&state.metrics.role, "unknown")),
            Span::raw("    "),
            Span::styled("AOF: ", Style::default().fg(Color::DarkGray)),
            Span::raw(if state.metrics.aof_enabled {
                "on"
            } else {
                "off"
            }),
        ]),
    ];

    let paragraph = Paragraph::new(lines)
        .block(Block::default().title("System").borders(Borders::ALL))
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, area);
}

fn draw_metrics(frame: &mut Frame<'_>, area: Rect, state: &MonitorState) {
    let metrics = &state.metrics;
    let items = vec![
        metric_item("Connected clients", metrics.connected_clients),
        metric_item("Key count", metrics.key_count),
        metric_item("Expired keys", metrics.expired_keys),
        metric_item("Memory bytes", metrics.memory_estimate_bytes),
        metric_item("Total commands", metrics.total_commands),
        metric_item("GET count", metrics.get_count),
        metric_item("SET count", metrics.set_count),
        metric_item("DEL count", metrics.del_count),
    ];

    let list = List::new(items).block(
        Block::default()
            .title("Key Metrics")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Blue)),
    );
    frame.render_widget(list, area);
}

fn draw_status(frame: &mut Frame<'_>, area: Rect, state: &MonitorState) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Length(7),
            Constraint::Min(4),
        ])
        .split(area);

    let gauge_style = match state.status {
        ConnectionStatus::Online => Style::default().fg(Color::Green),
        ConnectionStatus::Connecting => Style::default().fg(Color::Yellow),
        ConnectionStatus::Offline => Style::default().fg(Color::Red),
    };

    let gauge = Gauge::default()
        .block(
            Block::default()
                .title("Runtime Health")
                .borders(Borders::ALL)
                .border_style(gauge_style),
        )
        .gauge_style(gauge_style.add_modifier(Modifier::BOLD))
        .percent(state.health_percent())
        .label(format!("{}%", state.health_percent()));
    frame.render_widget(gauge, sections[0]);

    let qps = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("QPS: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{:.2}", state.qps),
                Style::default().fg(Color::Cyan),
            ),
        ]),
        Line::from(vec![
            Span::styled("Command mix: ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "GET {} / SET {} / DEL {}",
                state.metrics.get_count, state.metrics.set_count, state.metrics.del_count
            )),
        ]),
        Line::from(vec![
            Span::styled("Load note: ", Style::default().fg(Color::DarkGray)),
            Span::raw("INFO polling uses short-lived TCP connections"),
        ]),
        Line::from(vec![
            Span::styled("Frame limit: ", Style::default().fg(Color::DarkGray)),
            Span::raw(format_bytes(state.metrics.max_frame_size)),
        ]),
    ])
    .block(
        Block::default()
            .title("Traffic")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Magenta)),
    )
    .wrap(Wrap { trim: true });
    frame.render_widget(qps, sections[1]);

    let message = match &state.last_error {
        Some(error) => format!("Last error: {error}"),
        None => String::from("Server telemetry stream is healthy."),
    };

    let status = Paragraph::new(message)
        .block(Block::default().title("Status").borders(Borders::ALL))
        .wrap(Wrap { trim: true });
    frame.render_widget(status, sections[2]);
}

fn draw_help(frame: &mut Frame<'_>, area: Rect) {
    let help = Paragraph::new("q: quit safely | refresh: 1s | source: INFO")
        .style(Style::default().fg(Color::DarkGray))
        .block(Block::default().title("Help").borders(Borders::ALL));
    frame.render_widget(help, area);
}

fn metric_item(label: &'static str, value: u64) -> ListItem<'static> {
    ListItem::new(Line::from(vec![
        Span::styled(format!("{label:<20}"), Style::default().fg(Color::DarkGray)),
        Span::styled(
            value.to_string(),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
    ]))
}

fn default_text<'a>(value: &'a str, default: &'a str) -> &'a str {
    if value.is_empty() {
        default
    } else {
        value
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.1} MiB", bytes as f64 / 1024.0 / 1024.0)
    } else if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}
