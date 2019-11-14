import asyncio
import argcomplete
import logging
import os
import signal
import sys
from typing import (
    Sequence,
    Tuple,
    Type,
    TYPE_CHECKING,
)

from p2p.service import AsyncioManager

from trinity.exceptions import (
    AmbigiousFileSystem,
    MissingPath,
)
from trinity.initialization import (
    initialize_data_dir,
    is_data_dir_initialized,
)
from trinity.boot_info import BootInfo
from trinity.cli_parser import (
    parser,
    subparser,
)
from trinity.config import (
    BaseAppConfig,
    TrinityConfig,
)
from trinity.extensibility import (
    ApplicationComponentAPI,
    BaseComponentAPI,
)
from trinity.network_configurations import (
    PRECONFIGURED_NETWORKS,
)
from trinity._utils.logging import (
    enable_warnings_by_default,
    set_logger_levels,
    setup_file_logging,
    setup_stderr_logging,
    IPCListener,
)
from trinity._utils.version import (
    construct_trinity_client_identifier,
    is_prerelease,
)

if TYPE_CHECKING:
    from .main import TrinityMain  # noqa: F401


TRINITY_HEADER = "\n".join((
    "\n"
    r"      ______     _       _ __       ",
    r"     /_  __/____(_)___  (_) /___  __",
    r"      / / / ___/ / __ \/ / __/ / / /",
    r"     / / / /  / / / / / / /_/ /_/ / ",
    r"    /_/ /_/  /_/_/ /_/_/\__/\__, /  ",
    r"                           /____/   ",
))

TRINITY_AMBIGIOUS_FILESYSTEM_INFO = (
    "Could not initialize data directory\n\n"
    "   One of these conditions must be met:\n"
    "   * HOME environment variable set\n"
    "   * XDG_TRINITY_ROOT environment variable set\n"
    "   * TRINITY_DATA_DIR environment variable set\n"
    "   * --data-dir command line argument is passed\n"
    "\n"
    "   In case the data directory is outside of the trinity root directory\n"
    "   Make sure all paths are pre-initialized as Trinity won't attempt\n"
    "   to create directories outside of the trinity root directory\n"
)


def main_entry(trinity_service_class: Type['TrinityMain'],
               app_identifier: str,
               component_types: Tuple[Type[BaseComponentAPI], ...],
               sub_configs: Sequence[Type[BaseAppConfig]]) -> None:
    if is_prerelease():
        # this modifies the asyncio logger, but will be overridden by any custom settings below
        enable_warnings_by_default()

    for component_cls in component_types:
        component_cls.configure_parser(parser, subparser)

    argcomplete.autocomplete(parser)

    args = parser.parse_args()

    if not args.genesis and args.network_id not in PRECONFIGURED_NETWORKS:
        raise NotImplementedError(
            f"Unsupported network id: {args.network_id}. To use a network besides "
            "mainnet or ropsten, you must supply a genesis file with a flag, like "
            "`--genesis path/to/genesis.json`, also you must specify a data "
            "directory with `--data-dir path/to/data/directory`"
        )

    # The `common_log_level` is derived from `--log-level <Level>` / `-l <Level>` without
    # specifying any module. If present, it is used for both `stderr` and `file` logging.
    common_log_level = args.log_levels and args.log_levels.get(None)
    has_ambigous_logging_config = ((
        common_log_level is not None and
        args.stderr_log_level is not None
    ) or (
        common_log_level is not None and
        args.file_log_level is not None
    ))

    if has_ambigous_logging_config:
        parser.error(
            f"""\n
            Ambiguous logging configuration: The `--log-level (-l)` flag sets the
            log level for both file and stderr logging.
            To configure different log level for file and stderr logging,
            remove the `--log-level` flag and use `--stderr-log-level` and/or
            `--file-log-level` separately.
            Alternatively, remove the `--stderr-log-level` and/or `--file-log-level`
            flags to share one single log level across both handlers.
            """
        )

    try:
        trinity_config = TrinityConfig.from_parser_args(args, app_identifier, sub_configs)
    except AmbigiousFileSystem:
        parser.error(TRINITY_AMBIGIOUS_FILESYSTEM_INFO)

    if not is_data_dir_initialized(trinity_config):
        # TODO: this will only work as is for chains with known genesis
        # parameters.  Need to flesh out how genesis parameters for custom
        # chains are defined and passed around.
        try:
            initialize_data_dir(trinity_config)
        except AmbigiousFileSystem:
            parser.error(TRINITY_AMBIGIOUS_FILESYSTEM_INFO)
        except MissingPath as e:
            parser.error(
                "\n"
                f"It appears that {e.path} does not exist. "
                "Trinity does not attempt to create directories outside of its root path. "
                "Either manually create the path or ensure you are using a data directory "
                "inside the XDG_TRINITY_ROOT path"
            )

    # +---------------+
    # | LOGGING SETUP |
    # +---------------+

    # Setup logging to stderr
    stderr_logger_level = (
        args.stderr_log_level
        if args.stderr_log_level is not None
        else (common_log_level if common_log_level is not None else logging.INFO)
    )
    handler_stderr = setup_stderr_logging(stderr_logger_level)

    # Setup file based logging
    file_logger_level = (
        args.file_log_level
        if args.file_log_level is not None
        else (common_log_level if common_log_level is not None else logging.DEBUG)
    )
    handler_file = setup_file_logging(trinity_config.logfile_path, file_logger_level)

    # Set the individual logger levels that have been specified.
    logger_levels = {} if args.log_levels is None else args.log_levels
    set_logger_levels(logger_levels)

    # get the root logger and set it to the level of the stderr logger.
    logger = logging.getLogger()
    logger.setLevel(stderr_logger_level)

    # This prints out the ASCII "trinity" header in the terminal
    display_launch_logs(trinity_config)

    # Setup the log listener which child processes relay their logs through
    log_listener = IPCListener(handler_stderr, handler_file)

    # Determine what logging level child processes should use.
    child_process_log_level = min(
        stderr_logger_level,
        file_logger_level,
    )

    # Components can provide a subcommand with a `func` which does then control
    # the entire process from here.
    if hasattr(args, 'func'):
        args.func(args, trinity_config)
        return

    # This is a hack for the eth2.0 interop component
    if hasattr(args, 'munge_func'):
        args.munge_func(args, trinity_config)

    application_component_types = tuple(
        component_cls
        for component_cls
        in component_types
        if issubclass(component_cls, ApplicationComponentAPI)
    )

    boot_info = BootInfo(
        args=args,
        trinity_config=trinity_config,
        child_process_log_level=child_process_log_level,
        logger_levels=logger_levels,
        profile=bool(args.profile),
    )

    trinity_service = trinity_service_class(
        boot_info=boot_info,
        component_types=application_component_types,
    )

    manager = AsyncioManager(trinity_service)

    with log_listener.run(trinity_config.logging_ipc_path):
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGTERM, manager.cancel)
        loop.add_signal_handler(signal.SIGINT, manager.cancel)

        try:
            loop.run_until_complete(manager.run())
        except KeyboardInterrupt:
            pass
        finally:
            try:
                loop.run_until_complete(manager.wait_stopped())
            except KeyboardInterrupt:
                pass

            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except (KeyboardInterrupt, GeneratorExit):
                pass

            loop.stop()
            if trinity_config.trinity_tmp_root_dir:
                import shutil
                shutil.rmtree(trinity_config.trinity_root_dir)
    sys.exit(0)


def display_launch_logs(trinity_config: TrinityConfig) -> None:
    logger = logging.getLogger('trinity')
    logger.info(TRINITY_HEADER)
    logger.info("Started main process (pid=%d)", os.getpid())
    logger.info(construct_trinity_client_identifier())
    logger.info("Trinity DEBUG log file is created at %s", str(trinity_config.logfile_path))
