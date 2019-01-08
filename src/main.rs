#[macro_use]
extern crate serde_derive;
use chrono::{DateTime, Local};
use diesel::{Connection, RunQueryDsl};
use serde;
use std;
use std::path::{Path, PathBuf};

const MAX_RETRIES: usize = 4;

type DB = diesel::sqlite::Sqlite;
type DBConnection = diesel::SqliteConnection;

fn get_args() -> clap::ArgMatches<'static> {
    use clap::Arg;
    clap::App::new("Undelete Telegram messages in supergroup")
        .setting(clap::AppSettings::AllowLeadingHyphen)
        .arg(
            Arg::with_name("bot")
                .takes_value(true)
                .multiple(true)
                .long("bot")
                .number_of_values(1)
                .help("example: --bot 123:abcde/100000 --bot 456:fghij/200000 --bot 789:klmno")
                .required(true),
        )
        .arg(
            Arg::with_name("db")
                .takes_value(true)
                .long("db")
                .required(true),
        )
        .arg(
            Arg::with_name("chat-id")
                .takes_value(true)
                .long("chat-id")
                .required(true),
        )
        .arg(
            Arg::with_name("media-dir")
                .takes_value(true)
                .long("media-dir")
                .required(true),
        )
        .get_matches()
}

#[derive(Clone)]
struct Config {
    chat_id: i64,
    // [(user_id, bot_token)]
    bots: Vec<(Option<i64>, String)>,
    media_dir: String,
}

impl Config {
    fn parse_bots(args: &Vec<String>) -> Vec<(Option<i64>, String)> {
        let mut res = Vec::new();
        for arg in args {
            let mut split = arg.split("/");
            let token = split.next().expect("Invalid arg for token");
            let arg = split.next().map(|x| x.parse().unwrap());
            res.push((arg, token.into()))
        }
        res
    }
}

fn establish_connection(file: &str) -> DBConnection {
    diesel::sqlite::SqliteConnection::establish(file)
        .expect("Unable to establish sqlite connection to specified file")
}

#[derive(Debug)]
enum MediaType {
    Photo,
    Document,
    Webpage,
    Geo,
    Geolive,
    Contact,
    Venue,
}

#[derive(Debug)]
struct Media {
    id: i64,
    media_type: MediaType,
    mime_type: Option<String>,
    name: Option<String>,
    extra: String,
}

#[derive(Debug)]
struct Message {
    id: i64,
    user_name: String,
    user_id: i64,
    date: DateTime<Local>,
    reply_to: Option<i64>,
    reply_to_new_id: Option<i64>,
    text: String,
    media: Option<Media>,
}

#[derive(Deserialize)]
struct TelegramResp {
    #[used]
    ok: bool,
    result: WithMessageId,
}

#[derive(Deserialize)]
struct WithMessageId {
    message_id: i64,
}

impl std::fmt::Display for MediaType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                MediaType::Photo => "photo",
                MediaType::Document => "document",
                MediaType::Webpage => "webpage",
                MediaType::Geo => "geo",
                MediaType::Geolive => "geolive",
                MediaType::Venue => "venue",
                MediaType::Contact => "contact",
            }
        )
    }
}

impl Media {
    pub fn parse_row(
        id: Option<i64>,
        media_type: Option<String>,
        mime_type: Option<String>,
        name: Option<String>,
        extra: Option<String>,
    ) -> Option<Media> {
        if let None = id {
            return None;
        }
        let id = id.unwrap();

        let media_type =
            match media_type.expect("Media type not present").as_str() {
                "photo" => MediaType::Photo,
                "document" => MediaType::Document,
                "webpage" => MediaType::Webpage,
                "geo" => MediaType::Geo,
                "geolive" => MediaType::Geolive,
                "contact" => MediaType::Contact,
                "venue" => MediaType::Venue,
                t => panic!("Invalid media type {}", t),
            };

        let extra = extra.expect("Extra not present");

        Some(Media {
            id,
            media_type,
            mime_type,
            name,
            extra,
        })
    }

    fn caption(&self) -> String {
        use crate::MediaType::{Contact, Geo, Geolive, Venue};

        let prefix = match self.media_type {
            Geo | Geolive | Contact | Venue => format!("({})", self.media_type),
            _ => "".into(),
        };

        match &self.name {
            Some(name) => format!("{}\n{}", prefix, name),
            _ => prefix,
        }
    }

    fn caption_timestamped(&self, t: &DateTime<Local>) -> String {
        match self.media_type {
            MediaType::Photo => format!("{}", t.to_rfc3339()),
            _ => format!("{}\n{}", self.caption(), t.to_rfc3339()),
        }
    }

    fn find_file(&self, dir: &str) -> Option<PathBuf> {
        let pat = format!("{}/*/{}-*.{}.*", dir, self.media_type, self.id);
        let mut paths: Vec<PathBuf> = glob::glob(&pat)
            .unwrap()
            .map(std::result::Result::unwrap)
            .collect();
        match paths.len().clone() {
            0 => None,
            _ => paths.pop(),
        }
    }

    fn file_part(&self, dir: &str) -> Option<reqwest::multipart::Part> {
        let file = self.find_file(dir)?;
        let raw_part = reqwest::multipart::Part::file(&file).ok()?;
        let part_with_mime = reqwest::multipart::Part::file(&file)
            .ok()?
            .mime_str(self.mime_type.as_ref().unwrap_or(&"".into()))
            .ok();
        let part = part_with_mime.or(Some(raw_part))?;
        let file_name = Self::clean_filename(&file);
        // let file_name = "hello.jpg";
        let part_with_name = part.file_name(file_name);
        Some(part_with_name)
    }

    fn clean_filename<P: AsRef<Path>>(p: P) -> String {
        let file: &Path = p.as_ref().file_name().unwrap().as_ref();
        let real_ext = file.extension().unwrap();
        let stem_and_id: &Path = file.file_stem().unwrap().as_ref();
        let stem = stem_and_id
            .file_stem()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let pure_stem = stem
            .trim_start_matches("document-")
            .trim_start_matches("photo-");
        format!("{}.{}", pure_stem, real_ext.to_string_lossy())
    }
}

impl Message {
    fn send_request(&self, conf: &Config) -> Option<i64> {
        if self.media.is_none() {
            return self.send_text(conf);
        }

        let media = self.media.as_ref()?;

        let f = match &media.media_type {
            MediaType::Photo => Self::send_photo,
            MediaType::Document => Self::send_document,
            MediaType::Webpage => Self::send_webpage,
            _ => Self::send_other_media,
        };

        f(self, conf, &media)
    }

    fn send_photo(&self, conf: &Config, media: &Media) -> Option<i64> {
        let file = media.file_part(&conf.media_dir)?;
        self.request_telegram("sendPhoto", conf, |form| {
            form.text("caption", media.caption_timestamped(&self.date))
                .part("photo", file)
        })
    }

    fn send_webpage(&self, conf: &Config, media: &Media) -> Option<i64> {
        let t =
            format!("{}\n{}", self.text, media.caption_timestamped(&self.date));
        self.request_telegram("sendMessage", conf, |form| form.text("text", t))
    }

    fn send_document(&self, conf: &Config, media: &Media) -> Option<i64> {
        let file = media.find_file(&conf.media_dir)?;

        let file_size = std::fs::metadata(&file).unwrap().len();
        if file_size >= 50 * 1024 * 1024 {
            let file_name = Media::clean_filename(&*file);
            let caption = format!(
                "(oversized file: {} bytes)\n{}\n{}",
                file_size,
                file_name,
                media.caption_timestamped(&self.date)
            );
            return self.request_telegram("sendMessage", conf, |form| {
                form.text("caption", caption)
            });
        }

        let file_part = media.file_part(&conf.media_dir)?;
        self.request_telegram("sendDocument", conf, |form| {
            form.text("caption", media.caption_timestamped(&self.date))
                .part("document", file_part)
        })
    }

    fn send_other_media(&self, conf: &Config, media: &Media) -> Option<i64> {
        self.request_telegram("sendMessage", conf, |form| {
            form.text("text", media.caption_timestamped(&self.date))
        })
    }

    fn send_text(&self, conf: &Config) -> Option<i64> {
        self.request_telegram("sendMessage", conf, |form| {
            form.text("text", self.text_content(conf))
        })
    }

    // returns <user>:\n if necessary, or empty string
    fn from_user(&self, conf: &Config) -> String {
        let matched = conf.bots.iter().find(|&(x, _)| x == &Some(self.user_id));
        if matched.is_some() {
            "".into()
        } else {
            format!("{}:\n", self.user_name)
        }
    }

    fn request_telegram<F>(&self, api: &str, conf: &Config, f: F) -> Option<i64>
    where
        F: FnOnce(reqwest::multipart::Form) -> reqwest::multipart::Form,
    {
        // let proxy = reqwest::Proxy::https("http://127.0.0.1:8888").unwrap();
        // let client = reqwest::Client::builder().proxy(proxy).build().unwrap();
        let client = reqwest::Client::new();
        let token = self.pick_bot(conf);
        let url = format!("https://api.telegram.org/bot{}/{}", token, api);
        let form = reqwest::multipart::Form::new()
            .text("chat_id", (&conf.chat_id).to_string())
            .text("reply_to_message_id", self.reply_to_param());
        let form = f(form);

        let tmp = client.post(&url).multipart(form);

        let mut resp =
            tmp.send().ok().expect("Failed to send request to telegram");
        let msg_id = resp
            .json::<TelegramResp>()
            .expect("Failed to decode response from telegram")
            .result
            .message_id;
        Some(msg_id)
    }

    fn pick_bot(&self, conf: &Config) -> String {
        let matched = conf.bots.iter().find(|&(x, _)| x == &Some(self.user_id));
        let default = conf.bots.iter().find(|&(x, _)| x == &None);
        matched
            .or(default)
            .expect("at least something should match!")
            .1
            .clone()
    }

    fn reply_to_param(&self) -> String {
        match self.reply_to_new_id {
            None => "".into(),
            Some(i) => i.to_string(),
        }
    }

    fn text_content(&self, conf: &Config) -> String {
        let content = if self.text.len() > 0 {
            format!("{}{}", self.from_user(conf), self.text)
        } else {
            format!("(from {})", self.user_name)
        };
        format!("{}\n{}", content, self.format_date())
    }

    fn format_date(&self) -> String {
        self.date.to_rfc3339()
    }

    fn parse_reply_to_new_id(&mut self, conn: &DBConnection) {
        let old_id = match self.reply_to {
            None => return,
            Some(x) => x,
        };

        use diesel::sql_types::*;
        let new_id = diesel::dsl::sql::<BigInt>(
            "
            SELECT NewID
            FROM MessageIDMigration
            WHERE OldID = ?
            ",
        )
        .bind::<BigInt, _>(old_id)
        .get_result(conn)
        .ok();

        self.reply_to_new_id = new_id;
    }
}

impl diesel::deserialize::QueryableByName<DB> for Message {
    fn build<R: diesel::row::NamedRow<DB>>(
        row: &R,
    ) -> diesel::deserialize::Result<Self> {
        use chrono::{NaiveDateTime, TimeZone};
        use diesel::sql_types::*;

        let media = Media::parse_row(
            row.get::<Nullable<BigInt>, _>("media_id")?,
            row.get::<Nullable<Text>, _>("media_type")?,
            row.get::<Nullable<Text>, _>("media_mime_type")?,
            row.get::<Nullable<Text>, _>("media_name")?,
            row.get::<Nullable<Text>, _>("media_extra")?,
        );
        let naive_date =
            NaiveDateTime::from_timestamp(row.get::<BigInt, _>("date")?, 0);
        let date = Local.from_utc_datetime(&naive_date);

        Ok(Message {
            id: row.get::<BigInt, _>("message_id")?,
            user_name: row.get::<Text, _>("first_name")?,
            user_id: row.get::<BigInt, _>("user_id")?,
            date: date,
            reply_to: row.get::<Nullable<BigInt>, _>("reply_to")?,
            reply_to_new_id: None,
            text: row.get::<Text, _>("text")?,
            media: media,
        })
    }
}

fn pending_message_id(conn: &DBConnection) -> Option<i64> {
    use diesel::sql_types::*;
    diesel::dsl::sql::<BigInt>(
        "
        SELECT OldID
        FROM MessageIDMigration
        WHERE NewID IS NULL
        AND Retries <= 1
        ORDER BY UpdatedAt ASC, Retries ASC
        LIMIT 1
        ",
    )
    .bind::<BigInt, _>(MAX_RETRIES as i64)
    .get_result(conn)
    .ok()
}

fn vacant_message_id(conn: &DBConnection) -> Option<i64> {
    use diesel::sql_types::*;
    diesel::dsl::sql::<BigInt>(
        "
        SELECT ID
        FROM Message
        WHERE ID NOT IN (
            SELECT OldID
            FROM MessageIDMigration
        )
        AND ServiceAction IS NULL
        ORDER BY Date ASC
        LIMIT 1
        ",
    )
    .get_result(conn)
    .ok()
}

fn next_message_id(conn: &DBConnection) -> Option<i64> {
    pending_message_id(conn).or_else(|| vacant_message_id(conn))
}

fn fetch_message(conn: &DBConnection, id: i64) -> Option<Message> {
    use diesel::sql_types::*;

    diesel::sql_query(format!(
        "
        SELECT m.ID              AS message_id,
               u.FirstName       AS first_name,
               u.ID              AS user_id,
               m.ReplyMessageID  AS reply_to,
               m.Date            AS date,
               m.Message         AS text,
               p.ID              AS media_id,
               p.Type            AS media_type,
               p.MimeType        AS media_mime_type,
               p.Name            AS media_name,
               p.Extra           AS media_extra
        FROM MESSAGE AS m
        LEFT JOIN USER    AS u ON m.FromID  = u.ID
        LEFT JOIN MEDIA   AS p ON m.MediaID = p.ID
        WHERE m.ID >= ?
        AND m.ServiceAction IS NULL
        ORDER BY m.Date ASC
        LIMIT 1;
        ",
    ))
    .bind::<BigInt, _>(id)
    .get_result(conn)
    .expect("Unable to fetch next message")
}

fn increment_retries(conn: &DBConnection, old_id: i64) {
    use diesel::sql_types::*;
    let now_timestamp = Local::now().timestamp();
    diesel::sql_query(
        "
        UPDATE MessageIDMigration
        SET Retries = Retries + 1,
            UpdatedAt = ?
        WHERE OldID = ?
        ",
    )
    .bind::<BigInt, _>(now_timestamp)
    .bind::<BigInt, _>(old_id)
    .execute(conn)
    .expect("Unable to increment");
}
fn save_new_id(conn: &DBConnection, old_id: i64, new_id: i64) {
    use diesel::sql_types::*;
    let now_timestamp = Local::now().timestamp();
    diesel::sql_query(
        "
        UPDATE MessageIDMigration
        SET NewID = ?,
            UpdatedAt = ?
        WHERE OldID = ?
        ",
    )
    .bind::<BigInt, _>(new_id)
    .bind::<BigInt, _>(now_timestamp)
    .bind::<BigInt, _>(old_id)
    .execute(conn)
    .expect("Unable to save new id");
}

fn record_id_log(conn: &DBConnection, msg: &Message, new_id: Option<i64>) {
    use diesel::sql_types::*;
    let old_id = msg.id;
    let now_timestamp = Local::now().timestamp();

    diesel::sql_query(
        "
        INSERT INTO MessageIDMigration (OldID, UpdatedAt)
        VALUES (?, ?)
        ",
    )
    .bind::<BigInt, _>(old_id)
    .bind::<BigInt, _>(now_timestamp)
    .execute(conn)
    .ok();

    match new_id {
        None => increment_retries(conn, old_id),
        Some(new_id) => save_new_id(conn, old_id, new_id),
    };
}

#[allow(unused)]
fn send_message_by_id(conn: &DBConnection, conf: &Config, id: i64) {
    let msg = fetch_message(&conn, id).expect("Unable to fetch message");
    println!("Processing {}/{}", msg.user_name, msg.id);
    let id = msg.send_request(&conf);
    if id.is_none() {
        println!("Failed to process message: {:?}", msg)
    }
}

#[allow(unused)]
fn process_messages(conn: DBConnection, conf: Config) {
    loop {
        let conf = conf.clone();
        let id = match next_message_id(&conn) {
            None => break,
            Some(id) => id,
        };

        let mut msg =
            fetch_message(&conn, id).expect("Unable to fetch message");
        println!("Processing {}/{}", msg.user_name, msg.id);

        msg.parse_reply_to_new_id(&conn);
        let id = msg.send_request(&conf);
        if id.is_none() {
            println!("Failed to process message: {:?}", msg)
        }
        record_id_log(&conn, &msg, id);
    }
}

fn init_db_table(conn: &DBConnection) {
    conn.execute(
        "
        CREATE TABLE IF NOT EXISTS MessageIDMigration (
            ID INTEGER PRIMARY KEY,
            ContextID INTEGER,
            OldID INTEGER UNIQUE,
            NewID INTEGER,
            Retries INTEGER DEFAULT 0,
            UpdatedAt INTEGER
        )
        ",
    )
    .expect("Unable to create table");
}

fn main() {
    use clap::values_t;
    let args = get_args();
    let conn = establish_connection(args.value_of("db").unwrap());
    let bots =
        Config::parse_bots(&values_t!(args.values_of("bot"), String).unwrap());
    let conf = Config {
        chat_id: args
            .value_of("chat-id")
            .unwrap()
            .parse()
            .expect("Chat ID is not an integer"),
        bots: bots,
        media_dir: args.value_of("media-dir").unwrap().into(),
    };

    init_db_table(&conn);

    process_messages(conn, conf);
    // for debugging
    // send_message_by_id(&conn, &conf, 26390)
}
