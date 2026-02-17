
from scray.client.file_client import ScraySyncFileClient

client = ScraySyncFileClient("C:\\temp\\sync-api-stat.json.bk.22.04.2024.json")

print(len(client.load_versioned_data_from_file()))








