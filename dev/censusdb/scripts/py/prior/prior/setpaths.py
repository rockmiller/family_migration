# setpaths module: generated global variables and the folder map

from pathlib import Path
from loguru import logger

BASEPATH: Path = None  # Needed for generate_folder_map to access

def set_global_paths(path_tuples: list[tuple[str, str]]) -> dict[str, Path]:
    """
    Dynamically declares and sets global variables from a list of (name, path_string) tuples.
    Returns a dictionary of resolved Path objects.
    """
    resolved = {}

    for name, raw_path in path_tuples:
        try:
            path_obj = Path(raw_path).resolve()
            resolved[name] = path_obj

            # Dynamically declare and assign global variable
            globals()[name] = path_obj

            logger.info("Declared global {} = {}", name, path_obj)
        except Exception as e:
            logger.exception("Failed to configure {}: {}", name, e)
            raise

    return resolved

    
def generate_folder_map() -> dict[str, Path]:
    """
    Generates a dictionary mapping census years to folder paths under BASEPATH.
    Requires BASEPATH to be set globally.
    """
    if BASEPATH is None:
        raise ValueError("BASEPATH is not set. Run configure_paths() first.")

    years = [
        "1850", "1860", "1870", "1880", "1890",
        "1900", "1910", "1920"
    ]
    folder_map = {year: BASEPATH / f"cp{year}" for year in years}
    logger.info("Generated folder_map with {} entries under BASEPATH: {}", len(folder_map), BASEPATH)
    return folder_map