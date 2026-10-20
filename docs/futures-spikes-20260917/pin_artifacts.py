"""Pin sources, derived inputs, code and outcomes without including raw data."""
from pathlib import Path
import hashlib
import importlib.metadata
import json
import platform

ROOT = Path(__file__).resolve().parent


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


if __name__ == '__main__':
    environment = dict(python=platform.python_version(), packages={
        name: importlib.metadata.version(name)
        for name in ['numpy', 'pandas', 'numba', 'scipy', 'matplotlib', 'pytest']
    })
    (ROOT / 'data/environment.json').write_text(json.dumps(environment, indent=2))
    rows = []
    for name in ['manifest.json', 'param_sources.json']:
        for source in json.loads((ROOT / 'data' / name).read_text()):
            rows.append((source['sha256'], source['path']))
    paths = list((ROOT / 'cache').glob('*.npz'))
    paths += list(ROOT.glob('*.py')) + list(ROOT.glob('*.md'))
    paths += list((ROOT / 'data').glob('*.json'))
    paths += [path for path in (ROOT / 'out').iterdir() if path.is_file()]
    paths += [ROOT / name for name in [
        'extractor/src/main.rs', 'extractor/Cargo.toml', 'extractor/Cargo.lock',
        'PREREG.sha256', '.gitattributes', '.gitignore',
    ]]
    for path in sorted(paths):
        rows.append((digest(path), str(path.relative_to(ROOT))))
    (ROOT / 'data/pins.tsv').write_text(
        'sha256\tpath\n' + ''.join(f'{sha}\t{path}\n' for sha, path in rows))
    print(f'Pinned {len(rows)} files')
