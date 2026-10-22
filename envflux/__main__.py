import asyncio
import argparse
import logging
import signal

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

from .config import load_config
from .token_manager import TokenManager
from .failed_queue import FailedPointsQueue
from .tracker import InverterUpdateTracker
from .points import generate_points
from .influx import influx_write_points_with_queue
from pyenphase import Envoy

shutdown_event = asyncio.Event()
logger = logging.getLogger(__name__)
MAX_FAILED_POINTS = 10000


def _configured_envoys(config):
    entries = config.get("envoys")
    if not isinstance(entries, list) or len(entries) != 2:
        raise ValueError("Configure exactly two Envoy controllers in the envoys list")

    controllers = []
    names = set()
    for index, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            raise ValueError(f"Envoy entry {index} must be a mapping")
        host = entry.get("host")
        name = str(entry.get("name") or host or f"envoy-{index}")
        if not host:
            raise ValueError(f"Envoy entry {name} is missing host")
        if name in names:
            raise ValueError(f"Duplicate Envoy name: {name}")
        if not entry.get("token") and not (entry.get("username") and entry.get("password")):
            raise ValueError(f"Envoy {name} needs a token or username and password")
        names.add(name)
        controllers.append({
            "name": name,
            "config": entry,
            "client": None,
            "token_manager": None,
            "tracker": InverterUpdateTracker(),
        })
    return controllers


async def _connect_envoy(controller):
    config = controller["config"]
    envoy = Envoy(config["host"])
    try:
        await envoy.setup()
        token = config.get("token")
        if token:
            logger.info("Authenticating to Envoy %s with configured token", controller["name"])
            await envoy.authenticate(token=token)
            token_manager = None
        else:
            logger.info("Authenticating to Envoy %s with username and password", controller["name"])
            token_manager = TokenManager(envoy, cache_key=controller["name"])
            await token_manager.authenticate(config["username"], config["password"])
        return envoy, token_manager
    except Exception:
        client = getattr(envoy, "_client", None)
        if client and not client.closed:
            await client.close()
        raise


async def _close_envoy(envoy):
    client = getattr(envoy, "_client", None)
    if client and not client.closed:
        await client.close()


def _signal_handler(sig=None, frame=None):
    logger.info("Shutdown signal received, stopping...")
    shutdown_event.set()


def parse_args():
    parser = argparse.ArgumentParser(description="Envoy data collector")
    parser.add_argument(
        "-d", "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    parser.add_argument(
        "-c", "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration YAML file (default: config.yaml)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without sending data to InfluxDB, print points instead"
    )
    return parser.parse_args()


async def main(config_path="config.yaml", dry_run=False):
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    config = load_config(config_path)
    controllers = _configured_envoys(config)
    influx_url = config["influxdb"]["url"]
    influx_token = config["influxdb"]["token"]
    influx_org = config["influxdb"]["org"]
    influx_bucket = config["influxdb"]["bucket"]
    failed_points_queue = FailedPointsQueue(MAX_FAILED_POINTS)
    logger.info("Configured %d Envoy controller(s)", len(controllers))

    influx_client = None
    write_api = None
    if dry_run:
        logger.info("Running in dry-run mode. Skipping InfluxDB connection.")
    else:
        influx_client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)

    logger.info('Starting main loop')
    while not shutdown_event.is_set():
        for controller in controllers:
            try:
                if controller["client"] is None:
                    logger.info("Connecting to Envoy %s", controller["name"])
                    controller["client"], controller["token_manager"] = await _connect_envoy(controller)

                if controller["token_manager"]:
                    await controller["token_manager"].refresh_if_needed()

                data = await controller["client"].update()
                points = generate_points(data, controller["tracker"], controller["name"])

                for point in points:
                    logger.debug("Point from %s: %s", controller["name"], point.to_line_protocol())

                if not dry_run:
                    await influx_write_points_with_queue(write_api, influx_bucket, points, failed_points_queue)
            except Exception as e:
                logger.error("Failed to fetch or process data from Envoy %s: %s", controller["name"], e)
                await _close_envoy(controller["client"])
                controller["client"] = None
                controller["token_manager"] = None

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=30)
        except asyncio.TimeoutError:
            pass

    logger.info("Cleanup before shutdown...")
    if influx_client:
        write_api.flush()
        influx_client.close()
        logger.info("InfluxDB client closed")

    for controller in controllers:
        await _close_envoy(controller["client"])
    logger.info("Envoy HTTP sessions closed")

    logger.info("Shutdown complete")

    return

def main_entry():
    args = parse_args()
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level)
    logging.getLogger("pyenphase").setLevel(logging.INFO)

    asyncio.run(main(config_path=args.config, dry_run=args.dry_run))

if __name__ == "__main__":
    main_entry()
