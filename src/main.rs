// extern crate clap;
extern crate diesel;

#[macro_use]
extern crate serde_derive;
// extern crate futures;
// extern crate telegram_bot_fork;
// extern crate tokio;
//
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
        .arg(
            Arg::with_name("token")
                .takes_value(true)
                .long("token")
                .required(true),
        )
        .arg(
            Arg::with_name("db")
                .takes_value(true)
                .long("db")
                .required(true),
        )
        .arg(
            Arg::with_name("chat")
                .takes_value(true)
                .long("chat")
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
    token: String,
    media_dir: String,
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
    mime_type: String,
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

        let mime_type = mime_type.expect("MIME type not present");
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
        let s = format!("({})", self.media_type);
        match &self.name {
            Some(n) => format!("{} {}", s, n),
            None => s,
        }
    }

    fn caption_timestamped(&self, t: &DateTime<Local>) -> String {
        format!("{}\n{}", self.caption(), t.to_rfc3339())
    }

    fn find_file(&self, dir: &str) -> Option<PathBuf> {
        let pat = format!("{}/{}-*.{}.*", dir, self.media_type, self.id);
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
            .mime_str(&self.mime_type)
            .ok();
        let part = part_with_mime.or(Some(raw_part))?;
        let file_name = Self::clean_filename(&file);
        let part_with_name = part.file_name(file_name);
        Some(part_with_name)
    }

    fn clean_filename<P: AsRef<Path>>(p: P) -> String {
        let file: &Path = p.as_ref().file_name().unwrap().as_ref();
        let file_pure = if file.starts_with("document-") {
            file.strip_prefix("document-").unwrap()
        } else {
            file.strip_prefix("photo-").unwrap()
        };
        let real_ext = file_pure.extension().unwrap();
        let stem_and_id: &Path = file_pure.file_stem().unwrap().as_ref();
        let stem = stem_and_id.file_stem().unwrap();
        format!("{}.{}", stem.to_str().unwrap(), real_ext.to_str().unwrap())
    }
}

impl Message {
    fn send_request(&self, conf: &Config) -> Option<i64> {
        if self.media.is_none() {
            return self.send_text(conf);
        }

        let media = self.media.as_ref()?;

        match &media.media_type {
            MediaType::Photo => return self.send_photo(conf, &media),
            MediaType::Document => return self.send_document(conf, &media),
            _ => return self.send_other_media(conf, &media),
        }
    }

    fn send_photo(&self, conf: &Config, media: &Media) -> Option<i64> {
        let client = reqwest::Client::new();
        let file = media.file_part(&conf.media_dir)?;
        let url =
            format!("https://api.telegram.org/bot{}/sendPhoto", conf.token);
        let form = reqwest::multipart::Form::new()
            .text("chat_id", (&conf.chat_id).to_string())
            .text("caption", media.caption_timestamped(&self.date))
            .text("reply_to_message_id", self.reply_to_param())
            .part("photo", file);

        let mut resp = client.post(&url).multipart(form).send().ok()?;
        let msg_id = resp.json::<WithMessageId>().ok()?.message_id;
        Some(msg_id)
    }

    fn send_document(&self, conf: &Config, media: &Media) -> Option<i64> {
        let client = reqwest::Client::new();
        let file = media.file_part(&conf.media_dir)?;
        let url =
            format!("https://api.telegram.org/bot{}/sendDocument", conf.token);
        let form = reqwest::multipart::Form::new()
            .text("chat_id", (&conf.chat_id).to_string())
            .text("caption", media.caption_timestamped(&self.date))
            .text("reply_to_message_id", self.reply_to_param())
            .part("photo", file);

        let mut resp = client.post(&url).multipart(form).send().ok()?;
        let msg_id = resp.json::<WithMessageId>().ok()?.message_id;
        Some(msg_id)
    }

    fn send_other_media(&self, conf: &Config, media: &Media) -> Option<i64> {
        let client = reqwest::Client::new();
        let url =
            format!("https://api.telegram.org/bot{}/sendMessage", conf.token);
        let form = reqwest::multipart::Form::new()
            .text("chat_id", (&conf.chat_id).to_string())
            .text("text", media.caption_timestamped(&self.date))
            .text("reply_to_message_id", self.reply_to_param());

        let mut resp = client.post(&url).multipart(form).send().ok()?;
        let msg_id = resp.json::<WithMessageId>().ok()?.message_id;
        Some(msg_id)
    }

    fn send_text(&self, conf: &Config) -> Option<i64> {
        let client = reqwest::Client::new();
        let url =
            format!("https://api.telegram.org/bot{}/sendMessage", conf.token);
        let form = reqwest::multipart::Form::new()
            .text("chat_id", (&conf.chat_id).to_string())
            .text("text", self.text_content())
            .text("reply_to_message_id", self.reply_to_param());

        let mut resp = client.post(&url).multipart(form).send().ok()?;
        let msg_id = resp.json::<WithMessageId>().ok()?.message_id;
        Some(msg_id)
    }

    fn reply_to_param(&self) -> String {
        match self.reply_to_new_id {
            None => "".into(),
            Some(i) => i.to_string(),
        }
    }

    fn text_content(&self) -> String {
        let content = if self.text.len() > 0 {
            format!("{}:\n{}", self.user_name, self.text)
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
        use chrono::TimeZone;
        use diesel::sql_types::*;

        let media = Media::parse_row(
            row.get::<Nullable<BigInt>, _>("media_id")?,
            row.get::<Nullable<Text>, _>("media_type")?,
            row.get::<Nullable<Text>, _>("media_mime_type")?,
            row.get::<Nullable<Text>, _>("media_name")?,
            row.get::<Nullable<Text>, _>("media_extra")?,
        );
        let date = Local.from_utc_datetime(&row.get::<Timestamp, _>("date")?);

        Ok(Message {
            id: row.get::<BigInt, _>("id")?,
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
        ORDER BY Retries ASC, UpdatedAt ASC
        AND Retries <= ?
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
        ORDER BY Date ASC
        LIMIT 1
        ",
    )
    .get_result(conn)
    .ok()
}

fn fetch_next_message(conn: &DBConnection) -> Option<Message> {
    use diesel::sql_types::*;
    let next_msg_id =
        match pending_message_id(conn).or_else(|| vacant_message_id(conn)) {
            None => return None,
            Some(x) => x,
        };

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
        JOIN USER    AS u ON m.FromID  = u.ID
        JOIN MEDIA   AS p ON m.MediaID = p.ID
        ORDER BY ID ASC
        WHERE ID >= ?
        ",
    ))
    .bind::<BigInt, _>(next_msg_id)
    .get_result(conn)
    .ok()
}

fn increment_retries(conn: &DBConnection, old_id: i64) {
    use diesel::sql_types::*;
    diesel::sql_query(
        "
        UPDATE MessageIDMigration
        SET Retries = Retries + 1
        WHERE OldID = ?
        ",
    )
    .bind::<BigInt, _>(old_id)
    .execute(conn)
    .ok();
}
fn save_new_id(conn: &DBConnection, old_id: i64, new_id: i64) {
    use diesel::sql_types::*;
    diesel::sql_query(
        "
        UPDATE MessageIDMigration
        SET NewID = ?
        WHERE OldID = ?
        ",
    )
    .bind::<BigInt, _>(new_id)
    .bind::<BigInt, _>(old_id)
    .execute(conn)
    .ok();
}

fn record_id_log(conn: &DBConnection, msg: &Message, new_id: Option<i64>) {
    use diesel::sql_types::*;
    let old_id = msg.id;
    let now_timestamp = Local::now().timestamp();

    diesel::sql_query(
        "
        INSERT INTO MessageIDMigration (OldID, UpdatedAt)
        VALUES (?, ?)
        ON CONFLICT DO
        UPDATE SET UpdatedAt = ?
        ",
    )
    .bind::<BigInt, _>(old_id)
    .bind::<BigInt, _>(now_timestamp)
    .bind::<BigInt, _>(now_timestamp)
    .execute(conn)
    .ok();

    match new_id {
        None => increment_retries(conn, old_id),
        Some(new_id) => save_new_id(conn, old_id, new_id),
    };
}

fn process_messages(conn: DBConnection, conf: Config) {
    loop {
        let conf = conf.clone();
        let mut msg = match fetch_next_message(&conn) {
            None => break,
            Some(m) => m,
        };
        msg.parse_reply_to_new_id(&conn);
        let id = msg.send_request(&conf);
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
            Retries INTEGER DEFAULTS 0
            UpdatedAt INTEGER
        )
        ",
    )
    .ok();
}

fn main() {
    let args = get_args();
    let conn = establish_connection(args.value_of("db").unwrap());
    let conf = Config {
        chat_id: args
            .value_of("chat")
            .unwrap()
            .parse()
            .expect("Chat ID is not an integer"),
        token: args.value_of("token").unwrap().into(),
        media_dir: args.value_of("media-dir").unwrap().into(),
    };

    init_db_table(&conn);

    process_messages(conn, conf);
}
