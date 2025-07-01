import os
import json
import csv
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto
from dotenv import load_dotenv
from datetime import datetime

class TelegramScraper:
    def __init__(self, session_name='scraper_session'):
        load_dotenv('../.env')
        self.api_id = int(os.getenv('TG_API_ID'))
        self.api_hash = os.getenv('TG_API_HASH')
        self.phone = os.getenv('phone')
        self.client = TelegramClient(session_name, self.api_id, self.api_hash)
        self.media_dir = '../data/photos'
        self.raw_json_path = '../data/telegram_raw.json'
        self.csv_path = '../data/telegram_data.csv'
        os.makedirs(self.media_dir, exist_ok=True)

    async def connect(self):
        await self.client.start()

    async def scrape_channel(self, channel_username, writer, json_list):
        entity = await self.client.get_entity(channel_username)
        channel_title = entity.title

        async for message in self.client.iter_messages(entity, limit=10000):
            media_path = None
            if message.media and isinstance(message.media, MessageMediaPhoto):
                filename = f"{channel_username}_{message.id}.jpg"
                media_path = os.path.join(self.media_dir, filename)
                await self.client.download_media(message.media, media_path)

            msg_data = {
                "channel_title": channel_title,
                "channel_username": channel_username,
                "message_id": message.id,
                "text": message.message,
                "timestamp": message.date.isoformat(),
                "media_path": media_path,
                "views": getattr(message, "views", 0),
                "replies": getattr(message.replies, "replies", 0) if message.replies else 0,
                "sender_id": message.sender_id
            }

            json_list.append(msg_data)
            writer.writerow([
                msg_data["channel_title"], msg_data["channel_username"],
                msg_data["message_id"], msg_data["text"],
                msg_data["timestamp"], msg_data["media_path"],
                msg_data["views"], msg_data["replies"]
            ])

    async def scrape_channel(self, channel_username, writer, json_list, max_photos=100):
        entity = await self.client.get_entity(channel_username)
        channel_title = entity.title
        photo_count = 0  # Track number of photos downloaded for this channel

        async for message in self.client.iter_messages(entity, limit=10000):
            media_path = None

            # Download only up to max_photos
            if (
                message.media 
                and isinstance(message.media, MessageMediaPhoto)
                and photo_count < max_photos
            ):
                filename = f"{channel_username}_{message.id}.jpg"
                media_path = os.path.join(self.media_dir, filename)
                await self.client.download_media(message.media, media_path)
                photo_count += 1

            msg_data = {
                "channel_title": channel_title,
                "channel_username": channel_username,
                "message_id": message.id,
                "text": message.message,
                "timestamp": message.date.isoformat(),
                "media_path": media_path,
                "views": getattr(message, "views", 0),
                "replies": getattr(message.replies, "replies", 0) if message.replies else 0,
                "sender_id": message.sender_id
            }

            json_list.append(msg_data)
            if writer:
                writer.writerow([
                    msg_data["channel_title"], msg_data["channel_username"],
                    msg_data["message_id"], msg_data["text"],
                    msg_data["timestamp"], msg_data["media_path"],
                    msg_data["views"], msg_data["replies"]
                ])


    def run(self, channels):
        with self.client:
            self.client.loop.run_until_complete(self.scrape_channels(channels))
