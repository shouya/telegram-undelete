// extern crate clap;
#[macro_use]
extern crate diesel;
// extern crate futures;
// extern crate telegram_bot_fork;
// extern crate tokio;
//
use diesel::{Connection, RunQueryDsl};
use futures::{future, Future, Stream};
use std;
use telegram_bot_fork as tg;
use telegram_bot_fork_raw as tg_raw;

const MAX_RETRIES: usize = 4;

type DB = diesel::sqlite::Sqlite;
type DBConnection = diesel::SqliteConnection;
type TGRequest<T, K> = dyn tg::types::Request<
    Type = tg_raw::requests::_base::JsonRequestType<T>,
    Response = tg_raw::requests::_base::JsonIdResponse<K>,
>;
type TGMessageRequest<'a> = TGRequest<tg::SendMessage<'a>, tg::Message>;

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
        .get_matches()
}

struct Config {
    chat_id: String,
}

fn establish_connection(file: &str) -> DBConnection {
    diesel::sqlite::SqliteConnection::establish(file)
        .expect("Unable to establish sqlite connection to specified file")
}

enum MediaType {
    Photo,
    Document,
    Webpage,
    Geo,
    Geolive,
    Contact,
    Venue,
}

#[derive(diesel::Queryable)]
struct Media {
    id: i64,
    media_type: MediaType,
    mime_type: String,
    name: Option<String>,
    extra: String,
}

struct Message {
    id: i64,
    user_name: String,
    user_id: i64,
    reply_to: Option<i64>,
    text: String,
    media: Option<Media>,
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
}

impl diesel::deserialize::QueryableByName<DB> for Message {
    fn build<R: diesel::row::NamedRow<DB>>(
        row: &R,
    ) -> diesel::deserialize::Result<Self> {
        use diesel::sql_types::*;
        let media = Media::parse_row(
            row.get::<Nullable<BigInt>, _>("media_id")?,
            row.get::<Nullable<Text>, _>("media_type")?,
            row.get::<Nullable<Text>, _>("media_mime_type")?,
            row.get::<Nullable<Text>, _>("media_name")?,
            row.get::<Nullable<Text>, _>("media_extra")?,
        );

        Ok(Message {
            id: row.get::<BigInt, _>("id")?,
            user_name: row.get::<Text, _>("first_name")?,
            user_id: row.get::<BigInt, _>("user_id")?,
            reply_to: row.get::<Nullable<BigInt>, _>("reply_to")?,
            text: row.get::<Text, _>("text")?,
            media: media,
        })
    }
}

fn convert_id(old_id: i64, db: &DBConnection) -> Option<i64> {
    unimplemented!()
}

fn next_message_id_query(conn: &DBConnection) -> Option<i64> {
    use diesel::sql_types::*;
    diesel::dsl::sql::<BigInt>(
        "
        SELECT OldID
        FROM MessageIDMigration
        WHERE NewID IS NULL
        AND Retries <= {}
        ORDER BY UpdatedAt ASC
        LIMIT 1
        ",
    )
    .get_result(conn)
    .ok()
}

fn fetch_next_message(conn: &DBConnection) -> Option<Message> {
    use diesel::sql_types::*;
    let next_msg_id = match next_message_id_query(conn) {
        None => return None,
        Some(x) => x,
    };

    diesel::sql_query(format!(
        "
        SELECT m.ID              AS message_id,
               u.FirstName       AS first_name,
               u.ID              AS user_id,
               m.ReplyMessageID  AS reply_to,
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

fn send_request(
    bot: &tg::Api,
    req: Message,
) -> impl Future<Item = i64, Error = ()> {
    future::err(())
}

fn save_log(id: i64) -> impl Future<Item = (), Error = ()> {
    future::err(())
}

fn make_processing_stream(
    token: String,
    conn: DBConnection,
    conf: Config,
) -> impl Stream<Item = (), Error = ()> + Send {
    futures::stream::unfold((), move |()| {
        let bot = tg::Api::new(&token).unwrap();
        fetch_next_message(&conn).map(move |msg| {
            send_request(&bot, msg).and_then(save_log).map(|_| ((), ()))
        })
    })
}

mod schema {
    use diesel::*;

    table! {
        message {
            #[sql_name = "ID"]
            id -> BigInt,
            #[sql_name = "FromID"]
            from_id -> BigInt,
            #[sql_name = "MediaID"]
            media_id -> Nullable<BigInt>,
            #[sql_name = "Message"]
            text -> Text,
            #[sql_name = "ReplyID"]
            reply_id -> Nullable<BigInt>,
            #[sql_name = "ServiceAction"]
            service_action -> Nullable<Text>,
        }
    }

    table! {
        media {
            #[sql_name = "ID"]
            id -> BigInt,
            #[sql_name = "MIMEType"]
            mime_type -> Text,
            #[sql_name = "Name"]
            name -> Nullable<Text>,
            #[sql_name = "Extra"]
            extra -> Text,
        }
    }
}

fn init_db_table(conn: &DBConnection) {
    conn.execute(
        "
        CREATE TABLE IF NOT EXISTS MessageIDMigration (
            ID INTEGER PRIMARY KEY,
            ContextID INTEGER,
            OldID INTEGER,
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
    let token: String = args.value_of("token").unwrap().into();
    let conn = establish_connection(args.value_of("db").unwrap());
    let conf = Config {
        chat_id: args.value_of("chat").unwrap().into(),
    };

    init_db_table(&conn);

    let stream = make_processing_stream(token, conn, conf);
    tokio::run(stream.for_each(|_| future::ok(())));

    println!("Hello, world! {:?}", args);
}
