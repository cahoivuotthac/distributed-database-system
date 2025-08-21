from pathlib import Path

base_dir = Path(__file__).parent.parent 
staging_dir = base_dir / "staging"

staging_folders = {
	'raw': staging_dir / "raw",
	'transformed': staging_dir / "transformed",
	'validated': staging_dir / "validated" 
}

for folder in staging_folders.values():
	folder.mkdir(parents=True, exist_ok=True)