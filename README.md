[![Build Status](https://travis-ci.org/shouya/telegram-undelete.svg?branch=master)](https://travis-ci.org/shouya/telegram-undelete)

## Usage

1. Dump the message database and media files
  using [telegram-export](https://github.com/expectocode/telegram-export).
2. Update the message database manually using sqlite3 to remove any unwanted
  messages, you should preserve only one single conversation
  (a single chat or a group).
3. Create few bots as avatars to every major persons in the conversation with
  [@BotFather](https://t.me/BotFather).
4. Create another bot to represent the rest of the people in the group.
5. Create a group (or a supergroup) and add these bots into it.
6. Get the chat_id for the supergroup.
7. Get the major persons' user_id.
8. Run this program.

## Command line arguments

*All arguments are required.*

- `--chat-id`: the chat id of the group you wish the bots to send messages to
- `--bot`: this argument can appear multiple times to define each bot with parameter of format `<bot_token>[/<user_id>]`.
  - `bot_token`: bot token for the bot
  - `user_id`: user's id the bot represents, leave empty to represent all unmatched persons
- `--db`: the database of exported messages
- `--media-dir`: directory storing the photo and documents

## Example

```
telegram-undelete \
  --chat-id -341835998 \
  --db tg-export/export.db \
  --media-dir tg-export/usermedia/ \
  --bot 109827292:x2yCEKSN2CWsIIHCs3Av3BgSljVjsMOqwJ/95158592 \
  --bot 123918132:n9YhNQnssDRBGBZjS7tyhkpLFt3ISPQdOA/79878506 \
  --bot 119174829:qCf5254nn801BUoNyISil8hDtQpL8IkgAA
```

## Notes

- The program will mutate the sqlite database to save progress in a table named 'MessageIdMigration'.

- The program will abort when it encounters a network error. In this case, just restart the program and the progress will be resumed.

- The program runs at a rate of producing approximately 3 messages per second.

# LICENSE

This project is MIT licensed.


