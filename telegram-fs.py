from fuse import FUSE, FuseOSError, Operations
from telethon.sync import TelegramClient
from telethon.tl.types import InputMessagesFilterDocument
import config
import errno
import logging
import sys
import re

class TGFS(Operations):
    @property
    def default_file_attrs(self):
        return {
            'st_atime': 0,
            'st_ctime': 0,
            'st_mtime': 0,
            'st_mode': 0o100444,
            'st_nlink': 1,
            'st_uid': 0,
            'st_gid': 0,
            }

    @property
    def default_folder_attrs(self):
        return {
            'st_atime': 0,
            'st_ctime': 0,
            'st_mtime': 0,
            'st_mode': 0o40555,
            'st_nlink': 1,
            'st_size': 4096,
            'st_uid': 0,
            'st_gid': 0,
            }

    def __init__(self):
        self.client = TelegramClient(config.name, config.api_id, config.api_hash, proxy=config.proxy)
        self.file_diz = {}
        self.client.connect()
        print('started')

    def __get__(self, path):
        if matches := re.search('/([^/]*)(?:/([^/]*))?(?:/([^/]*))?', path):
            return matches.groups()

    def __cache_file_attrs__(self, message):
        if self.file_diz.get(message.chat_id) is None:
            self.file_diz[message.chat_id] = {}
        if message.file:
            self.file_diz[message.chat_id][message.id] = {
                'file_name': message.file.name,
                'size': message.file.size,
                'timestamp': int(message.media.document.date.timestamp()),
                }
            return self.file_diz[message.chat_id][message.id]
        else:
            logging.error(f'Error while processing chat_id {message.chat_id} message_id {message.id}')

    def getattr(self, path, fh=None):
        logging.info(f'getattr: {path}, {fh}')
        _dialog_id, _message_id, _media = self.__get__(path)
        if _media:
            _attrs = self.file_diz.get(int(_dialog_id), {}).get(int(_message_id))
            if _attrs is None:
                for message in self.client.iter_messages(entity = int(_dialog_id), ids=int(_message_id), filter = InputMessagesFilterDocument):
                    _attrs = self.__cache_file_attrs__(message)
            if _media == _attrs['file_name']:
                return self.default_file_attrs | {
                    'st_atime': _attrs['timestamp'],
                    'st_ctime': _attrs['timestamp'],
                    'st_mtime': _attrs['timestamp'],
                    'st_size': _attrs['size'],
                    }
        else:
            return self.default_folder_attrs

    def readdir(self, path, fh):
        logging.info(f'readdir: {path}, {fh}')
        _dialog_id, _message_id, _media = self.__get__(path)
        if _message_id:
            _attrs = self.file_diz.get(int(_dialog_id), {}).get(int(_message_id))
            if _attrs is None:
                for message in self.client.iter_messages(entity = int(_dialog_id), ids=int(_message_id), filter = InputMessagesFilterDocument):
                    _attrs = self.__cache_file_attrs__(message)
                    yield(_attrs['file_name'])
            else:
                yield(_attrs['file_name'])
        elif _dialog_id:

            for message in self.client.iter_messages(entity = int(_dialog_id), filter = InputMessagesFilterDocument):
                if self.file_diz.get(int(_dialog_id), {}).get(message.id) is None:
                    self.__cache_file_attrs__(message)
                else:
                    break
            for _message_id in self.file_diz.get(int(_dialog_id)).keys():
                yield(str(_message_id))
        else:
            for dialog in self.client.iter_dialogs():
                if self.file_diz.get(int(dialog.id)) is None:
                    self.file_diz[int(dialog.id)] = {}
                else:
                    break
            for _dialog_id in self.file_diz.keys():
                yield(str(_dialog_id))

    def open(self, path, flags):
        logging.info(f'open: {path} - {flags}')
        if flags & 0o100000:
            return 0
        else:
            return -errno.EACCES

    def read(self, path, size, offset, fh):
        logging.info(f'read: {path} - {size} - {offset} - {fh}')
        _dialog_id, _message_id, _media = self.__get__(path)
        for message in self.client.iter_messages(entity = int(_dialog_id), ids=int(_message_id), filter = InputMessagesFilterDocument):
            for chunk in self.client.iter_download(file = message, offset = offset, limit = 1, request_size = size):
                return chunk

def main(mountpoint):
    FUSE(TGFS(), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[1])
