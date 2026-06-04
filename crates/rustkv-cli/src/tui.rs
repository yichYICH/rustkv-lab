use std::error::Error;
use std::io;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Wrap};
use rustkv_protocol::parser::parse_resp;
use rustkv_protocol::ProtocolError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::{encode_command, format_resp};

const MAX_HISTORY_LINES: usize = 500;

pub(crate) async fn run_shell(addr: String) -> Result<(), Box<dyn Error>> {
    let mut client = ShellClient::connect(&addr).await?;

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    if let Err(error) = execute!(stdout, EnterAlternateScreen) {
        let _ = disable_raw_mode();
        return Err(error.into());
    }

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let mut app = ShellApp::new(addr);
    app.push_system("Connected with one persistent TCP session.");
    app.push_system("Type help for commands, quit to leave the shell.");

    let result = run_loop(&mut terminal, &mut app, &mut client).await;
    let cleanup = restore_terminal(&mut terminal);

    match (result, cleanup) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error.into()),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn run_loop<B: Backend>(
    terminal: &mut Terminal<B>,
    app: &mut ShellApp,
    client: &mut ShellClient,
) -> Result<(), Box<dyn Error>> {
    loop {
        terminal.draw(|frame| draw(frame, app))?;

        if !event::poll(Duration::from_millis(80))? {
            continue;
        }

        let Event::Key(key) = event::read()? else {
            continue;
        };

        if key.kind != KeyEventKind::Press {
            continue;
        }

        match key.code {
            KeyCode::Esc => break,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => break,
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                app.input.clear();
            }
            KeyCode::Backspace => {
                app.input.pop();
            }
            KeyCode::Enter => match submit_input(app, client).await? {
                true => {}
                false => break,
            },
            KeyCode::Char(ch) => {
                app.input.push(ch);
            }
            _ => {}
        }
    }

    Ok(())
}

async fn submit_input(
    app: &mut ShellApp,
    client: &mut ShellClient,
) -> Result<bool, Box<dyn Error>> {
    let line = app.input.trim().to_owned();
    app.input.clear();

    if line.is_empty() {
        return Ok(true);
    }

    app.push_user(&line);

    match parse_shell_input(&line) {
        Ok(ShellAction::Quit) => Ok(false),
        Ok(ShellAction::Clear) => {
            app.history.clear();
            app.push_system("History cleared.");
            Ok(true)
        }
        Ok(ShellAction::Help) => {
            for line in help_lines() {
                app.push_system(line);
            }
            Ok(true)
        }
        Ok(ShellAction::Send(args)) => {
            app.connected = true;
            match client.execute(&args).await {
                Ok(response) => {
                    app.request_count += 1;
                    app.last_error = None;
                    app.push_response(&response);
                }
                Err(error) => {
                    app.connected = false;
                    app.last_error = Some(error.to_string());
                    app.push_error(&format!("request failed: {error}"));
                }
            }
            Ok(true)
        }
        Err(error) => {
            app.push_error(&error);
            Ok(true)
        }
    }
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> io::Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

fn draw(frame: &mut Frame<'_>, app: &ShellApp) {
    let area = frame.size();
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Min(8),
            Constraint::Length(3),
            Constraint::Length(3),
        ])
        .split(area);

    draw_header(frame, vertical[0], app);

    let middle = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(68), Constraint::Percentage(32)])
        .split(vertical[1]);

    draw_history(frame, middle[0], app);
    draw_help_panel(frame, middle[1], app);
    draw_input(frame, vertical[2], app);
    draw_footer(frame, vertical[3]);
}

fn draw_header(frame: &mut Frame<'_>, area: Rect, app: &ShellApp) {
    let status = if app.connected { "ONLINE" } else { "ERROR" };
    let status_style = if app.connected {
        Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
    };
    let lines = vec![
        Line::from(vec![
            Span::styled(
                "rustkv interactive client",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(status, status_style),
        ]),
        Line::from(vec![
            Span::styled("Address: ", Style::default().fg(Color::DarkGray)),
            Span::raw(app.addr.as_str()),
            Span::raw("    "),
            Span::styled("Session: ", Style::default().fg(Color::DarkGray)),
            Span::raw("persistent TCP"),
            Span::raw("    "),
            Span::styled("Requests: ", Style::default().fg(Color::DarkGray)),
            Span::raw(app.request_count.to_string()),
        ]),
        Line::from(vec![
            Span::styled("Last error: ", Style::default().fg(Color::DarkGray)),
            Span::raw(app.last_error.as_deref().unwrap_or("none")),
        ]),
    ];

    let paragraph = Paragraph::new(lines)
        .block(Block::default().title("Session").borders(Borders::ALL))
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, area);
}

fn draw_history(frame: &mut Frame<'_>, area: Rect, app: &ShellApp) {
    let visible_width = area.width.saturating_sub(2) as usize;
    let visible_height = area.height.saturating_sub(2) as usize;
    let text = app.visible_history_text(visible_width, visible_height);

    let paragraph = Paragraph::new(text).block(
        Block::default()
            .title("Command Stream")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Blue)),
    );

    frame.render_widget(paragraph, area);
}

fn draw_help_panel(frame: &mut Frame<'_>, area: Rect, app: &ShellApp) {
    let items = vec![
        help_item("PING"),
        help_item("SET name rust"),
        help_item("GET name"),
        help_item("DEL name [other]"),
        help_item("EXISTS name"),
        help_item("KEYS"),
        help_item("EXPIRE name 30"),
        help_item("TTL name"),
        help_item("INFO"),
        help_item("FLUSHDB"),
        help_item("clear / help / quit"),
    ];

    let title = if app.connected {
        "Commands"
    } else {
        "Commands - reconnect by restarting shell"
    };

    let list = List::new(items).block(
        Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Magenta)),
    );

    frame.render_widget(list, area);
}

fn draw_input(frame: &mut Frame<'_>, area: Rect, app: &ShellApp) {
    let block = Block::default()
        .title("Input")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Green));
    let inner = block.inner(area);
    let visible = app.visible_input(inner.width as usize);
    let input_len = visible.chars().count() as u16;

    let paragraph = Paragraph::new(visible)
        .style(Style::default().fg(Color::White))
        .block(block);
    frame.render_widget(paragraph, area);

    let cursor_x = inner.x + input_len.min(inner.width.saturating_sub(1));
    frame.set_cursor(cursor_x, inner.y);
}

fn draw_footer(frame: &mut Frame<'_>, area: Rect) {
    let footer = Paragraph::new("Enter: send command | Esc/Ctrl+C: quit | Ctrl+U: clear input")
        .style(Style::default().fg(Color::DarkGray))
        .block(Block::default().title("Help").borders(Borders::ALL));
    frame.render_widget(footer, area);
}

fn help_item(text: &'static str) -> ListItem<'static> {
    ListItem::new(Line::from(Span::raw(text)))
}

struct ShellApp {
    addr: String,
    input: String,
    history: Vec<String>,
    connected: bool,
    request_count: u64,
    last_error: Option<String>,
}

impl ShellApp {
    fn new(addr: String) -> Self {
        Self {
            addr,
            input: String::new(),
            history: Vec::new(),
            connected: true,
            request_count: 0,
            last_error: None,
        }
    }

    fn push_user(&mut self, text: &str) {
        self.push_history(format!("> {text}"));
    }

    fn push_response(&mut self, text: &str) {
        for line in text.lines() {
            self.push_history(format!("< {line}"));
        }
    }

    fn push_error(&mut self, text: &str) {
        self.push_history(format!("! {text}"));
    }

    fn push_system(&mut self, text: &str) {
        self.push_history(format!("* {text}"));
    }

    fn push_history(&mut self, line: String) {
        self.history.push(line);
        if self.history.len() > MAX_HISTORY_LINES {
            let overflow = self.history.len() - MAX_HISTORY_LINES;
            self.history.drain(..overflow);
        }
    }

    fn visible_input(&self, width: usize) -> String {
        let max_width = width.saturating_sub(1);
        if max_width == 0 {
            return String::new();
        }

        let chars = self.input.chars().collect::<Vec<_>>();
        let start = chars.len().saturating_sub(max_width);
        chars[start..].iter().collect()
    }

    fn visible_history_text(&self, width: usize, height: usize) -> String {
        if width == 0 || height == 0 {
            return String::new();
        }

        let rows = self
            .history
            .iter()
            .flat_map(|line| wrap_history_line(line, width))
            .collect::<Vec<_>>();
        let start = rows.len().saturating_sub(height);
        rows[start..].join("\n")
    }
}

fn wrap_history_line(line: &str, width: usize) -> Vec<String> {
    if width == 0 {
        return Vec::new();
    }

    let mut rows = Vec::new();
    let mut current = String::new();
    let mut current_width = 0;

    for ch in line.chars() {
        if current_width == width {
            rows.push(current);
            current = String::new();
            current_width = 0;
        }

        current.push(ch);
        current_width += 1;
    }

    if !current.is_empty() || line.is_empty() {
        rows.push(current);
    }

    rows
}

struct ShellClient {
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl ShellClient {
    async fn connect(addr: &str) -> Result<Self, Box<dyn Error>> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream,
            buffer: Vec::with_capacity(4096),
        })
    }

    async fn execute(&mut self, args: &[Vec<u8>]) -> Result<String, Box<dyn Error>> {
        let request = encode_command(args);
        self.stream.write_all(&request).await?;
        self.stream.flush().await?;
        self.read_response_text().await
    }

    async fn read_response_text(&mut self) -> Result<String, Box<dyn Error>> {
        let mut chunk = [0_u8; 4096];

        loop {
            match parse_resp(&self.buffer) {
                Ok((frame, consumed)) => {
                    let text = format_resp(&frame);
                    self.buffer.drain(..consumed);
                    return Ok(text);
                }
                Err(ProtocolError::Incomplete) => {
                    let bytes_read = self.stream.read(&mut chunk).await?;
                    if bytes_read == 0 {
                        return Err(
                            "server closed connection before sending a complete RESP response"
                                .into(),
                        );
                    }
                    self.buffer.extend_from_slice(&chunk[..bytes_read]);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}

enum ShellAction {
    Send(Vec<Vec<u8>>),
    Clear,
    Help,
    Quit,
}

fn parse_shell_input(line: &str) -> Result<ShellAction, String> {
    let tokens = split_command_line(line)?;
    if tokens.is_empty() {
        return Ok(ShellAction::Help);
    }

    match tokens[0].to_ascii_lowercase().as_str() {
        "quit" | "exit" => Ok(ShellAction::Quit),
        "clear" => Ok(ShellAction::Clear),
        "help" | "?" => Ok(ShellAction::Help),
        _ => Ok(ShellAction::Send(
            tokens.into_iter().map(String::into_bytes).collect(),
        )),
    }
}

fn split_command_line(input: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut active = false;

    for ch in input.chars() {
        if escaped {
            current.push(ch);
            active = true;
            escaped = false;
            continue;
        }

        if ch == '\\' {
            escaped = true;
            active = true;
            continue;
        }

        if let Some(quote_ch) = quote {
            if ch == quote_ch {
                quote = None;
            } else {
                current.push(ch);
            }
            active = true;
            continue;
        }

        if matches!(ch, '"' | '\'') {
            quote = Some(ch);
            active = true;
            continue;
        }

        if ch.is_whitespace() {
            if active {
                tokens.push(current.clone());
                current.clear();
                active = false;
            }
            continue;
        }

        current.push(ch);
        active = true;
    }

    if escaped {
        current.push('\\');
    }

    if let Some(quote_ch) = quote {
        return Err(format!("missing closing quote {quote_ch}"));
    }

    if active {
        tokens.push(current);
    }

    Ok(tokens)
}

fn help_lines() -> &'static [&'static str] {
    &[
        "Examples:",
        "  ping",
        "  set name rust",
        "  set greeting \"hello rust\"",
        "  get name",
        "  del name",
        "  expire name 30",
        "  ttl name",
        "  info",
        "Local commands: help, clear, quit",
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn visible_history_text_keeps_latest_response_after_wrapped_line() {
        let mut app = ShellApp::new(String::from("127.0.0.1:6379"));
        app.push_response(
            "{\"server_version\":\"0.1.0\",\"total_commands\":1,\"connected_clients\":1}",
        );
        app.push_response("PONG");

        let text = app.visible_history_text(16, 3);

        assert!(text.ends_with("< PONG"));
        assert!(text.lines().count() <= 3);
    }

    #[test]
    fn del_with_one_key_is_a_complete_command() {
        let ShellAction::Send(args) = parse_shell_input("del name").unwrap() else {
            panic!("expected del to be sent to the server");
        };

        assert_eq!(args, vec![b"del".to_vec(), b"name".to_vec()]);
    }
}
