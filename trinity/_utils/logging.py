import contextlib
import copy
import errno
import logging
from logging import (
    StreamHandler
)
from logging.handlers import (
    RotatingFileHandler,
)
import os
from pathlib import Path
import socket
import sys
import threading
from typing import (
    Dict,
    Iterator,
    Type,
    TypeVar,
)

import cloudpickle

from trinity._utils.shellart import (
    bold_red,
    bold_yellow,
)
from trinity._utils.socket import BufferedSocket
from trinity._utils.ipc import wait_for_ipc

LOG_BACKUP_COUNT = 10
LOG_MAX_MB = 5


THandler = TypeVar("THandler", bound="IPCHandler")


class IPCHandler(logging.Handler):
    logger = logging.getLogger('trinity._utils.logging.IPCHandler')

    def __init__(self, sock: socket.socket):
        self._socket = BufferedSocket(sock)
        super().__init__()

    @classmethod
    def connect(cls: Type[THandler], path: Path) -> THandler:
        wait_for_ipc(path)
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        cls.logger.debug("Opened connection to %s: %s", path, s)
        s.connect(str(path))
        return cls(s)

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        msg = self.format(record)
        new_record = copy.copy(record)
        new_record.message = msg
        new_record.msg = msg
        new_record.args = None
        new_record.exc_info = None
        new_record.exc_text = None
        return new_record

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg_data = cloudpickle.dumps(self.prepare(record))
            msg_length_data = len(msg_data).to_bytes(4, 'big')
            self._socket.sendall(msg_length_data + msg_data)
        except Exception:
            self.handleError(record)


class IPCListener:
    logger = logging.getLogger('trinity._utils.logging.IPCListener')

    def __init__(self, *handlers: logging.Handler) -> None:
        self._started = threading.Event()
        self._stopped = threading.Event()
        self.handlers = handlers

    @property
    def is_started(self) -> bool:
        return self._started.is_set()

    @property
    def is_running(self) -> bool:
        return self.is_started and not self.is_stopped

    @property
    def is_stopped(self) -> bool:
        return self._stopped.is_set()

    def wait_started(self) -> None:
        self._started.wait()

    def wait_stopped(self) -> None:
        self._stopped.wait()

    def start(self, ipc_path: Path) -> None:
        threading.Thread(
            name=f"log-listener:{ipc_path}",
            target=self.serve,
            args=(ipc_path,),
            daemon=False,
        ).start()
        self.wait_started()

    def stop(self) -> None:
        self._stopped.set()

    def _close_socket_on_stop(self, sock: socket.socket) -> None:
        # This function runs in the background waiting for the `stop` Event to
        # be set at which point it closes the socket, causing the server to
        # shutdown.  This allows the server threads to be cleanly closed on
        # demand.
        self.wait_stopped()

        try:
            sock.shutdown(socket.SHUT_RD)
        except OSError as e:
            # on mac OS this can result in the following error:
            # OSError: [Errno 57] Socket is not connected
            if e.errno != errno.ENOTCONN:
                raise

        sock.close()

    @contextlib.contextmanager
    def run(self, ipc_path: Path) -> Iterator['IPCListener']:
        self.start(ipc_path)
        try:
            yield self
        finally:
            self.stop()

            if ipc_path.exists():
                ipc_path.unlink()

    def serve(self, ipc_path: Path) -> None:
        self.logger.debug("Starting database server over IPC socket: %s", ipc_path)

        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            # background task to close the socket.
            threading.Thread(
                name="_close_socket_on_stop",
                target=self._close_socket_on_stop,
                args=(sock,),
                daemon=False,
            ).start()

            # These options help fix an issue with the socket reporting itself
            # already being used since it accepts many client connection.
            # https://stackoverflow.com/questions/6380057/python-binding-socket-address-already-in-use
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(str(ipc_path))
            sock.listen(1)

            self._started.set()

            while self.is_running:
                try:
                    conn, addr = sock.accept()
                except (ConnectionAbortedError, OSError) as err:
                    self.logger.debug("Server stopping: %s", err)
                    self._stopped.set()
                    break
                self.logger.debug('Server accepted connection: %r', addr)
                threading.Thread(
                    name="_serve_conn",
                    target=self._serve_conn,
                    args=(conn,),
                    daemon=False,
                ).start()

    def _serve_conn(self, raw_socket: socket.socket) -> None:
        self.logger.debug("%s: starting client handler for %s", self, raw_socket)

        with raw_socket:
            sock = BufferedSocket(raw_socket)

            while self.is_running:
                try:
                    length_data = sock.read_exactly(4)
                except OSError as err:
                    self.logger.debug("%s: closing client connection: %s", self, raw_socket)
                    break
                except Exception:
                    self.logger.exception("Error reading operation flag")
                    break

                data_length = int.from_bytes(length_data, 'big')

                try:
                    record_bytes = sock.read_exactly(data_length)
                except OSError as err:
                    self.logger.debug("%s: closing client connection: %s", self, raw_socket)
                    break
                except Exception:
                    self.logger.exception("Error reading operation flag")
                    break

                record = cloudpickle.loads(record_bytes)
                self.handle(record)

    def handle(self, record: logging.LogRecord) -> None:
        """
        Handle a record.
        This just loops through the handlers offering them the record
        to handle.
        """
        for handler in self.handlers:
            if record.levelno >= handler.level:
                handler.handle(record)


class TrinityLogFormatter(logging.Formatter):

    def __init__(self, fmt: str) -> None:
        super().__init__(fmt)

    def format(self, record: logging.LogRecord) -> str:
        record.shortname = record.name.split('.')[-1]  # type: ignore

        if record.levelno >= logging.ERROR:
            return bold_red(super().format(record))
        elif record.levelno >= logging.WARNING:
            return bold_yellow(super().format(record))
        else:
            return super().format(record)


LOG_FORMATTER = TrinityLogFormatter(
    fmt='%(levelname)8s  %(asctime)s  %(shortname)20s  %(message)s',
)


def set_logger_levels(log_levels: Dict[str, int],
                      *handlers: logging.Handler) -> None:
    for name, level in log_levels.items():

        # The root logger is configured separately
        if name is None:
            continue

        logger = logging.getLogger(name)
        logger.propagate = False
        logger.setLevel(level)

        for handler in handlers:
            handler.setLevel(level)
            handler.setFormatter(LOG_FORMATTER)

            logger.addHandler(handler)


def setup_stderr_logging(level: int=None) -> StreamHandler:

    if level is None:
        level = logging.INFO
    logger = logging.getLogger()

    handler_stream = logging.StreamHandler(sys.stderr)
    handler_stream.setLevel(level)

    handler_stream.setFormatter(LOG_FORMATTER)

    logger.addHandler(handler_stream)

    logger.debug('Logging initialized for stderr: PID=%s', os.getpid())

    return handler_stream


def setup_file_logging(
        logfile_path: Path,
        level: int=None) -> RotatingFileHandler:
    if level is None:
        level = logging.DEBUG
    logger = logging.getLogger()

    handler_file = RotatingFileHandler(
        str(logfile_path),
        maxBytes=(10000000 * LOG_MAX_MB),
        backupCount=LOG_BACKUP_COUNT
    )

    handler_file.setLevel(level)
    handler_file.setFormatter(LOG_FORMATTER)

    logger.addHandler(handler_file)

    return handler_file


def setup_child_process_logging(ipc_path: Path, level: int) -> None:
    # We get the root logger here to ensure that all logs are given a chance to
    # pass through this handler
    logger = logging.getLogger()
    logger.setLevel(level)

    ipc_handler = IPCHandler.connect(ipc_path)
    ipc_handler.setLevel(level)

    logger.addHandler(ipc_handler)

    logger.debug(
        'Logging initialized for file %s: PID=%s',
        ipc_path.resolve(),
        os.getpid(),
    )


def _set_environ_if_missing(name: str, val: str) -> None:
    """
    Set the environment variable so that other processes get the changed value.
    """
    if os.environ.get(name, '') == '':
        os.environ[name] = val


def enable_warnings_by_default() -> None:
    """
    This turns on some python and asyncio warnings, unless
    the related environment variables are already set.
    """
    _set_environ_if_missing('PYTHONWARNINGS', 'default')
    # PYTHONASYNCIODEBUG is not turned on by default because it slows down sync a *lot*
    logging.getLogger('asyncio').setLevel(logging.DEBUG)
