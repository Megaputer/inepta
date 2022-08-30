"""
...
"""
import _thread
import argparse
import base64
import contextlib
import enum
import io
import json
import logging
import math
import sys
import threading
import time
import uuid
from configparser import ConfigParser
from pathlib import Path

__all__ = ['ColumnType', 'Node', 'parser']


class ColumnType(enum.Enum):
    Numerical = '$num'
    Integer = '$num_int'
    Boolean = '$bool'
    String = '$cat_string'
    DateTime = '$num_datetime'
    StringID = '$cat'
    IntegerID = '$id'
    Text = '$text'


class _DefaultIni:
    ConfigParser.optionxform = str  # make case-sensitive

    @staticmethod
    def loads(s):
        cp = ConfigParser(allow_no_value=True)
        cp.read_string(s)
        return dict(cp['DEFAULT'])

    @staticmethod
    def dumps(d):
        buf = io.StringIO()
        ConfigParser(defaults=d).write(buf)
        buf.seek(0)
        return buf.read()


parser = _DefaultIni  # todo


class Node:
    """
    ...
    not thread/async safe!
    """

    def __init__(self, description=None, columns=None, parameters=None, reset_url_semantic=False):
        self._attrs = (description, columns, parameters, reset_url_semantic)

        self.name = Path(sys.argv[0]).stem
        self.bulk_size = 10
        self._buffer = []

    def __enter__(self):
        self._cfg = _handle_cli(*self._attrs)
        self._quota = self._cfg['maximum_rows'] or math.inf

        _setup_logging(self._cfg['log_folder'], self._cfg['debug_mode'], self.name)

        # ...
        def _wait():
            stop_file = Path(self._cfg['output_folder']) / 'STOP'
            while not stop_file.exists():
                time.sleep(1)
            _thread.interrupt_main()
        threading.Thread(target=_wait, daemon=True).start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._flush()  # won't have effect when the scraper is cancelled by user
        if exc_type is KeyboardInterrupt:
            logging.warning('User aborted execution')
            return True
        elif exc_type is _RowsLimitExceeded:
            logging.warning(f'The scraper exceeded the rows limit: {self._cfg["maximum_rows"]}')
            return True
        else:
            logging.exception('The scraper ended with an error')

    @property
    def url(self):
        return self._cfg['url']

    @property
    def parameters(self):
        return self._cfg['params']

    @property
    def proxy(self):
        schemes = {
            0: 'http',
            1: 'socks4',
            2: 'socks5',
            3: 'socks4a',
            4: 'http1.0',
            5: 'socks5h',
        }
        with contextlib.suppress(KeyError):
            proxy = self._cfg['proxy']
            proxy['type'] = schemes[proxy['type']]
            return proxy

    def add(self, url, title=None, content=None, **columns):
        """
        ...
        :raises _RowsLimitExceeded when hit maximum_rows number
        """
        if isinstance(content, str):
            content = content.encode()

        self._buffer.append({
            'url': url,
            'title': title,
            'content': base64.standard_b64encode(content).decode('ascii'),
            'columns': columns,
        })
        self._quota -= 1
        if not self._quota > 0:
            raise _RowsLimitExceeded

        if len(self._buffer) >= self.bulk_size:
            self._flush()

    def _flush(self):
        self._buffer, rows = [], self._buffer
        out_file = Path(self._cfg['output_folder']) / f'{uuid.uuid4()}.json'

        out_file.with_suffix('.lock').touch()
        with open(out_file, mode='w', encoding='utf_8') as f:
            json.dump({'docs': rows}, f)
        out_file.with_suffix('.lock').unlink()


class _RowsLimitExceeded(BaseException):
    pass


def _handle_cli(description, columns, parameters, reset_url_semantic):
    # define cli
    cli = argparse.ArgumentParser(add_help=False, description='A web scraper for PolyAnalyst')
    cli.add_argument(
        '-h',
        action='help',
        help='show this help message and exit'
    )
    cli.add_argument(
        dest='file',
        metavar='FILE',
        help='configuration file',
        type=argparse.FileType(mode='r+', encoding='utf_8'),
    )
    group = cli.add_mutually_exclusive_group()
    group.add_argument(
        '--help',
        action='store_true',
        help='write web scraper description to FILE and exit',
    )
    group.add_argument(
        '--features',
        action='store_true',
        help='write web scraper features to FILE and exit',
    )

    # handle cli
    args = cli.parse_args()

    if not(args.help or args.features):  # main run
        data = json.load(args.file)
        data['params'] = parser.loads(data['params'])
        args.file.close()
        return data

    # rewrite file
    args.file.seek(0)
    args.file.truncate()

    if args.help:
        args.file.write(description)

    if args.features:
        if callable(columns):
            _params = json.load(args.file)['params']
            columns = columns(parser.loads(_params))

        features = {
            'columns': [{'name': k, 'type': v.value} for k, v in columns.items()],
            'params': parser.dumps(parameters),
            'reset_url_semantic': reset_url_semantic,
        }
        json.dump(features, args.file)

    args.file.close()
    sys.exit()


def _setup_logging(log_folder, is_debug, name):
    file = Path(log_folder) / f'scraper_{name}_{uuid.uuid4().hex}.log'

    logger = logging.getLogger()
    if is_debug:  # todo
        logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(file, encoding='utf8')
    handler.setFormatter(
        logging.Formatter(
            fmt='%(asctime)s.%(msecs)3d %(name)s %(levelname)s %(message)s',
            datefmt='%H:%M:%S',
        )
    )
    logger.addHandler(handler)
