import os
import re
import json
import vdf
import requests
from PIL import Image
import io
import subprocess
import time
import psutil
import logging
import argparse
import sys
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Set, Tuple
from dotenv import load_dotenv


# ----------------------------
# Logging + Utils
# ----------------------------
def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("sunshine_automation.log"),
        ],
    )


def normalize_path(path: str) -> str:
    if not path:
        return path

    # Unescape common Windows double-backslashes coming from env files
    path = path.replace("\\\\", "\\")

    # Expand env vars + user home first
    path = os.path.expandvars(path)
    path = os.path.expanduser(path)

    # Normalize
    path = os.path.normpath(path)
    return path


# ----------------------------
# Env / Config
# ----------------------------
def _load_env() -> None:
    """
    Load .env from:
      - current working directory (default python-dotenv behavior)
      - AND from the directory where this script lives (more reliable)
    override=True so .env wins over the shell when you're debugging.
    """
    load_dotenv(override=True)
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        env_path = os.path.join(script_dir, ".env")
        if os.path.exists(env_path):
            load_dotenv(dotenv_path=env_path, override=True)
    except Exception:
        # Fallback: don't fail just because __file__ isn't available in some contexts
        pass


def validate_config() -> Dict[str, str]:
    _load_env()

    def getenv_first(*names: str) -> Optional[str]:
        for name in names:
            v = os.getenv(name)
            if v:
                return v
        return None

    # canonical -> accepted aliases (backward compatible)
    required_vars = {
        "STEAM_LIBRARY_VDF_PATH": (
            "STEAM_LIBRARY_VDF_PATH",
            "steam_library_vdf_path",
            "library_vdf_path",
        ),
        "SUNSHINE_APPS_JSON_PATH": (
            "SUNSHINE_APPS_JSON_PATH",
            "sunshine_apps_json_path",
            "apps_json_path",
        ),
        "SUNSHINE_GRIDS_FOLDER": (
            "SUNSHINE_GRIDS_FOLDER",
            "sunshine_grids_folder",
            "grids_folder",
        ),
        "STEAMGRIDDB_API_KEY": (
            "STEAMGRIDDB_API_KEY",
            "steamgriddb_api_key",
        ),
    }

    descriptions = {
        "STEAM_LIBRARY_VDF_PATH": "Steam library VDF file path",
        "SUNSHINE_APPS_JSON_PATH": "Sunshine apps.json file path",
        "SUNSHINE_GRIDS_FOLDER": "Sunshine grids folder path",
        "STEAMGRIDDB_API_KEY": "SteamGridDB API key",
    }

    config: Dict[str, str] = {}
    missing = []

    for canonical, aliases in required_vars.items():
        value = getenv_first(*aliases)
        if not value:
            missing.append(f"{canonical} ({descriptions[canonical]})")
            continue

        if canonical.endswith("_PATH") or canonical.endswith("_FOLDER"):
            value = normalize_path(value)
            logging.debug(f"Normalized {canonical}: {value}")

        config[canonical] = value

    # Optional (Windows restart support)
    steam_exe = getenv_first("STEAM_EXE_PATH", "steam_exe_path") or ""
    sunshine_exe = getenv_first("SUNSHINE_EXE_PATH", "sunshine_exe_path") or ""
    config["STEAM_EXE_PATH"] = normalize_path(steam_exe) if steam_exe else ""
    config["SUNSHINE_EXE_PATH"] = normalize_path(sunshine_exe) if sunshine_exe else ""

    if missing:
        logging.error("Missing required environment variables: " + ", ".join(missing))
        sys.exit(1)

    if not os.path.exists(config["STEAM_LIBRARY_VDF_PATH"]):
        logging.error(f"Steam library VDF file not found: {config['STEAM_LIBRARY_VDF_PATH']}")
        sys.exit(1)

    apps_dir = os.path.dirname(config["SUNSHINE_APPS_JSON_PATH"])
    if apps_dir and not os.path.exists(apps_dir):
        logging.error(f"Sunshine config directory not found: {apps_dir}")
        logging.info("Please ensure Sunshine is installed and has created its config directory")
        sys.exit(1)

    return config


# ----------------------------
# Steam/Sunshine restart (Windows only)
# ----------------------------
def restart_steam(steam_exe_path: str) -> None:
    if os.name != "nt":
        logging.warning("Steam restarting is only supported on Windows. Please restart Steam manually if needed.")
        return

    if not steam_exe_path or not os.path.exists(steam_exe_path):
        logging.warning("Steam executable path not configured or doesn't exist. Skipping Steam restart.")
        return

    logging.info("Restarting Steam...")
    try:
        terminated = False
        for proc in psutil.process_iter(["name", "pid"]):
            if proc.info["name"] and proc.info["name"].lower() == "steam.exe":
                logging.debug(f"Terminating Steam process (PID: {proc.info['pid']})")
                proc.terminate()
                try:
                    proc.wait(timeout=30)
                    terminated = True
                except psutil.TimeoutExpired:
                    logging.warning(f"Steam process (PID: {proc.info['pid']}) didn't terminate gracefully")
                    proc.kill()

        if terminated:
            time.sleep(3)

        logging.info(f"Starting Steam from: {steam_exe_path}")
        subprocess.Popen([steam_exe_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(10)
        logging.info("Steam restart completed")

    except Exception as e:
        logging.error(f"Error restarting Steam: {e}")


def restart_sunshine(sunshine_exe_path: str) -> None:
    if os.name != "nt":
        logging.warning("Sunshine restarting is only supported on Windows. Please restart Sunshine manually.")
        return

    if not sunshine_exe_path or not os.path.exists(sunshine_exe_path):
        logging.warning("Sunshine executable path not configured or doesn't exist. Skipping Sunshine restart.")
        return

    logging.info("Restarting Sunshine...")
    try:
        terminated = False
        for proc in psutil.process_iter(["name", "pid"]):
            if proc.info["name"] and proc.info["name"].lower() == "sunshine.exe":
                logging.debug(f"Terminating Sunshine process (PID: {proc.info['pid']})")
                proc.terminate()
                try:
                    proc.wait(timeout=30)
                    terminated = True
                except psutil.TimeoutExpired:
                    logging.warning(f"Sunshine process (PID: {proc.info['pid']}) didn't terminate gracefully")
                    proc.kill()

        if terminated:
            time.sleep(3)

        logging.info(f"Starting Sunshine from: {sunshine_exe_path}")
        subprocess.Popen([sunshine_exe_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logging.info("Sunshine restart completed")

    except Exception as e:
        logging.error(f"Error restarting Sunshine: {e}")


# ----------------------------
# Sunshine apps helpers (Steam AppID extraction + dedupe)
# ----------------------------
_STEAM_RUNGAME_RE = re.compile(r"steam://rungameid/(\d+)", re.IGNORECASE)


def extract_steam_app_id(cmd: str) -> Optional[str]:
    if not cmd:
        return None
    m = _STEAM_RUNGAME_RE.search(cmd)
    return m.group(1) if m else None


def _score_app_for_keep(app: Dict) -> int:
    """
    Prefer entries that look "better" when duplicates exist.
    Higher score wins.
    """
    score = 0
    if app.get("image-path"):
        score += 10
    if app.get("name"):
        score += 3
    # prefer cmd that contains flatpak/steam wrapper over raw? neutral
    return score


def dedupe_sunshine_apps(apps: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    Remove duplicates:
      - Steam apps: same AppID
      - Non-Steam: same (name, cmd)
    Keeps the "best" Steam entry when duplicates exist (prefers image-path).
    Returns (deduped, removed).
    """
    deduped: List[Dict] = []
    removed: List[Dict] = []

    steam_index: Dict[str, int] = {}
    other_seen: Set[Tuple[str, str]] = set()

    for app in apps:
        cmd = (app.get("cmd") or "").strip()
        name = (app.get("name") or "").strip()
        steam_id = extract_steam_app_id(cmd)

        if steam_id:
            if steam_id not in steam_index:
                steam_index[steam_id] = len(deduped)
                deduped.append(app)
            else:
                i = steam_index[steam_id]
                current = deduped[i]
                if _score_app_for_keep(app) > _score_app_for_keep(current):
                    removed.append(current)
                    deduped[i] = app
                else:
                    removed.append(app)
        else:
            key = (name, cmd)
            if key in other_seen:
                removed.append(app)
            else:
                other_seen.add(key)
                deduped.append(app)

    return deduped, removed


# ----------------------------
# Steam API + SteamGridDB
# ----------------------------
@lru_cache(maxsize=1000)
def get_game_name(app_id: str) -> Optional[str]:
    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"

    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if str(app_id) in data and data[str(app_id)].get("success"):
                game_data = data[str(app_id)].get("data", {})
                name = game_data.get("name")
                if name:
                    logging.debug(f"Retrieved name for AppID {app_id}: {name}")
                    return name

            logging.warning(f"No valid data found for AppID {app_id}")
            return None

        except requests.exceptions.Timeout:
            logging.warning(f"Timeout fetching name for AppID {app_id} (attempt {attempt + 1}/3)")
        except requests.exceptions.RequestException as e:
            logging.warning(f"Request error for AppID {app_id} (attempt {attempt + 1}/3): {e}")
        except Exception as e:
            logging.error(f"Unexpected error fetching name for AppID {app_id}: {e}")
            return None

        if attempt < 2:
            time.sleep(2 ** attempt)

    logging.error(f"Failed to fetch name for AppID {app_id} after 3 attempts")
    return None


def fetch_grid_from_steamgriddb(app_id: str, api_key: str, grids_folder: str) -> Optional[str]:
    url = f"https://www.steamgriddb.com/api/v2/grids/steam/{app_id}"
    headers = {"Authorization": f"Bearer {api_key}"}

    for attempt in range(3):
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if "data" in data and data["data"]:
                grid_url = data["data"][0]["url"]
                grid_resp = requests.get(grid_url, timeout=30)
                grid_resp.raise_for_status()

                try:
                    image = Image.open(io.BytesIO(grid_resp.content))
                    image.verify()
                    image = Image.open(io.BytesIO(grid_resp.content))

                    os.makedirs(grids_folder, exist_ok=True)
                    grid_path = os.path.join(grids_folder, f"{app_id}.png")
                    image.save(grid_path, "PNG")
                    logging.debug(f"Downloaded grid for AppID {app_id}: {grid_path}")
                    return grid_path

                except Exception as img_error:
                    logging.warning(f"Invalid image data for AppID {app_id}: {img_error}")
                    return None

            logging.warning(f"No grid data found for AppID {app_id}")
            return None

        except requests.exceptions.Timeout:
            logging.warning(f"Timeout fetching grid for AppID {app_id} (attempt {attempt + 1}/3)")
        except requests.exceptions.RequestException as e:
            logging.warning(f"Request error for AppID {app_id} (attempt {attempt + 1}/3): {e}")
        except Exception as e:
            logging.error(f"Unexpected error fetching grid for AppID {app_id}: {e}")
            return None

        if attempt < 2:
            time.sleep(2 ** attempt)

    logging.error(f"Failed to fetch grid for AppID {app_id} after 3 attempts")
    return None


# ----------------------------
# Sunshine config read/write
# ----------------------------
def get_sunshine_config(path: str) -> Dict:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                config = json.load(f)

            if not isinstance(config, dict):
                raise ValueError("Config must be a dictionary")

            config.setdefault("apps", [])
            config.setdefault("env", "")

            logging.info(f"Loaded Sunshine config with {len(config['apps'])} apps")
            return config

        logging.info("Sunshine config not found, initializing empty config")
        return {"env": "", "apps": []}

    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in Sunshine config file: {e}")
        raise
    except Exception as e:
        logging.error(f"Error loading Sunshine config: {e}")
        raise


def save_sunshine_config(path: str, config: Dict) -> None:
    try:
        if os.path.exists(path):
            import shutil
            backup_path = f"{path}.backup"
            shutil.copy2(path, backup_path)
            logging.debug(f"Created backup: {backup_path}")

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4, ensure_ascii=False)

        logging.info(f"Saved Sunshine config with {len(config.get('apps', []))} apps")
    except Exception as e:
        logging.error(f"Error saving Sunshine config: {e}")
        raise


# ----------------------------
# Steam library parsing + Sunshine update
# ----------------------------
def load_installed_games(library_vdf_path: str) -> Dict[str, str]:
    logging.info(f"Loading Steam library from {library_vdf_path}")

    try:
        with open(library_vdf_path, "r", encoding="utf-8") as f:
            steam_data = vdf.load(f)
    except Exception as e:
        logging.error(f"Error loading Steam library VDF: {e}")
        raise

    installed_games: Dict[str, str] = {}
    total_apps = 0

    libraryfolders = steam_data.get("libraryfolders", {})
    for folder_data in libraryfolders.values():
        if isinstance(folder_data, dict) and "apps" in folder_data and isinstance(folder_data["apps"], dict):
            total_apps += len(folder_data["apps"])

    logging.info(f"Processing {total_apps} Steam apps...")

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_app_id = {}

        for folder_data in libraryfolders.values():
            if not (isinstance(folder_data, dict) and "apps" in folder_data and isinstance(folder_data["apps"], dict)):
                continue
            for app_id in folder_data["apps"].keys():
                future = executor.submit(get_game_name, str(app_id))
                future_to_app_id[future] = str(app_id)

        processed = 0
        for future in as_completed(future_to_app_id):
            app_id = future_to_app_id[future]
            processed += 1

            try:
                name = future.result()
                if name:
                    installed_games[app_id] = name
            except Exception as e:
                logging.warning(f"Error processing AppID {app_id}: {e}")

            if processed % 50 == 0 or processed == total_apps:
                logging.info(f"Processed {processed}/{total_apps} apps...")

    logging.info(f"Found {len(installed_games)} installed games")
    return installed_games


def process_existing_apps(
    sunshine_config: Dict,
    installed_games: Dict[str, str],
) -> Tuple[List[Dict], List[Tuple[str, str]], Set[str]]:
    updated_apps: List[Dict] = []
    removed_games: List[Tuple[str, str]] = []
    existing_steam_apps: Set[str] = set()

    for app in sunshine_config.get("apps", []):
        cmd = app.get("cmd", "") or ""
        app_id = extract_steam_app_id(cmd)

        if app_id:
            if app_id in installed_games:
                updated_apps.append(app)
                existing_steam_apps.add(app_id)
            else:
                removed_games.append((app.get("name", "Unknown"), app_id))
                grid_path = app.get("image-path")
                if grid_path and os.path.exists(grid_path):
                    try:
                        os.remove(grid_path)
                        logging.debug(f"Removed grid image: {grid_path}")
                    except Exception as e:
                        logging.warning(f"Failed to remove grid image {grid_path}: {e}")
        else:
            updated_apps.append(app)

    return updated_apps, removed_games, existing_steam_apps


def add_new_games(new_games: Set[str], installed_games: Dict[str, str], api_key: str, grids_folder: str) -> List[Dict]:
    new_apps: List[Dict] = []
    if not new_games:
        return new_apps

    logging.info(f"Adding {len(new_games)} new games...")

    def build_cmd(app_id: str) -> str:
        if os.name == "nt":
            return f"steam://rungameid/{app_id}"

        # Linux / others: prefer Flatpak if installed
        try:
            fp = subprocess.run(
                ["flatpak", "list", "--app", "--columns=application"],
                capture_output=True,
                text=True,
                check=False,
            )
            if "com.valvesoftware.Steam" in (fp.stdout or ""):
                return f"flatpak run com.valvesoftware.Steam steam://rungameid/{app_id}"
        except FileNotFoundError:
            pass

        return f"steam steam://rungameid/{app_id}"

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_app_id = {
            executor.submit(fetch_grid_from_steamgriddb, app_id, api_key, grids_folder): app_id
            for app_id in new_games
        }

        processed = 0
        for future in as_completed(future_to_app_id):
            app_id = future_to_app_id[future]
            processed += 1

            try:
                grid_path = future.result()
                game_name = installed_games[app_id]

                new_apps.append(
                    {
                        "name": game_name,
                        "cmd": build_cmd(app_id),
                        "output": "",
                        "detached": "",
                        "elevated": "false",
                        "hidden": "true",
                        "wait-all": "true",
                        "exit-timeout": "5",
                        "image-path": grid_path or "",
                    }
                )
                logging.info(f"Added: {game_name}")

            except Exception as e:
                logging.error(f"Error processing new game {app_id}: {e}")

            if processed % 10 == 0 or processed == len(new_games):
                logging.info(f"Processed {processed}/{len(new_games)} new games...")

    return new_apps


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Sunshine Steam Game Automation")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--no-restart", action="store_true", help="Skip restarting Steam and Sunshine")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    args = parser.parse_args()

    setup_logging(args.verbose)
    logging.info("Starting Sunshine Steam Game Automation")

    try:
        config = validate_config()

        if not args.no_restart:
            restart_steam(config["STEAM_EXE_PATH"])

        installed_games = load_installed_games(config["STEAM_LIBRARY_VDF_PATH"])

        sunshine_config = get_sunshine_config(config["SUNSHINE_APPS_JSON_PATH"])

        # 1) Deduplicate existing Sunshine config first
        sunshine_config["apps"], removed_dupes = dedupe_sunshine_apps(sunshine_config.get("apps", []))
        if removed_dupes:
            logging.info(f"Removed {len(removed_dupes)} duplicate entries from Sunshine config")

        # Ensure grids folder exists
        os.makedirs(config["SUNSHINE_GRIDS_FOLDER"], exist_ok=True)

        # 2) Remove uninstalled + detect existing Steam AppIDs
        updated_apps, removed_games, existing_steam_apps = process_existing_apps(sunshine_config, installed_games)

        # 3) Compute new games to add
        new_games = set(installed_games.keys()) - existing_steam_apps

        if removed_games:
            logging.info(f"Games to remove: {[name for name, _ in removed_games]}")
        if new_games:
            logging.info(f"New games to add: {[installed_games[app_id] for app_id in new_games]}")

        changes_needed = bool(removed_dupes or removed_games or new_games)

        if not changes_needed:
            logging.info("No changes needed - all games are up to date")
            return

        if args.dry_run:
            logging.info("Dry run mode - no changes will be made")
            return

        # 4) Add new games
        new_apps = add_new_games(new_games, installed_games, config["STEAMGRIDDB_API_KEY"], config["SUNSHINE_GRIDS_FOLDER"])
        updated_apps.extend(new_apps)

        # 5) Final dedupe (safety net), then save
        updated_apps, removed_after = dedupe_sunshine_apps(updated_apps)
        if removed_after:
            logging.info(f"Removed {len(removed_after)} duplicates after update (safety net)")

        sunshine_config["apps"] = updated_apps
        save_sunshine_config(config["SUNSHINE_APPS_JSON_PATH"], sunshine_config)

        if not args.no_restart:
            restart_sunshine(config["SUNSHINE_EXE_PATH"])

        logging.info("Sunshine apps.json update process completed successfully")

    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
