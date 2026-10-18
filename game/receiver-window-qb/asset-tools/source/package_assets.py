"""Create the downloadable football archive after validating the actual GLBs."""
import argparse
import hashlib
import json
from pathlib import Path
import zipfile


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pack', type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = args.pack.resolve()
    output = args.output.resolve()
    if output.is_relative_to(root):
        raise ValueError('The ZIP output must be outside the pack directory.')
    manifest = json.loads((root / 'manifest.json').read_text(encoding='utf-8'))
    if len(manifest['models']) < 16 or len({m['id'] for m in manifest['models']}) != len(manifest['models']):
        raise ValueError('At least 16 distinct model entries are required.')
    report = json.loads((root / 'validation' / 'summary.json').read_text(encoding='utf-8'))
    if report.get('partial') or report.get('passed') is not True or report.get('khronosErrors') != 0:
        raise ValueError('Full-pack validation must pass before packaging.')
    for name in ['three-load-smoke.json', 'animation-semantics.json', 'physique-compatibility.json', 'player-loader-import.json']:
        check = json.loads((root / 'validation' / name).read_text(encoding='utf-8'))
        if check.get('partial') or check.get('passed') is not True:
            raise ValueError(f'Required acceptance check did not pass: {name}')
    for asset in report['assets']:
        target = (root / asset['file']).resolve()
        if not target.is_relative_to(root) or digest(target) != asset['sha256']:
            raise ValueError(f'GLB changed after validation: {asset["file"]}')
    required = ['README.md', 'LICENSE.txt', 'preview/index.html', 'preview/viewer.js', 'preview/physique.js']
    for entry in manifest['models']:
        required.extend([entry['path'], entry['lodPath']])
    for relative in required:
        if not (root / relative).is_file():
            raise ValueError(f'Missing deliverable: {relative}')
    files = sorted(p for p in root.rglob('*') if p.is_file() and 'node_modules' not in p.parts and '__pycache__' not in p.parts and p.name != 'SHA256SUMS.txt' and not p.name.endswith('.blend1'))
    expected = {file.relative_to(root).as_posix(): digest(file) for file in files}
    checksums = ''.join(f'{value}  {name}\n' for name, value in expected.items())
    sums = root / 'SHA256SUMS.txt'
    sums.write_text(checksums, encoding='utf-8')
    files.append(sums)
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for file in files:
            archive.write(file, file.relative_to(root).as_posix())
    with zipfile.ZipFile(output) as archive:
        if archive.testzip() is not None or len(archive.namelist()) != len(files):
            raise ValueError('ZIP integrity check failed.')
        for name, value in expected.items():
            if hashlib.sha256(archive.read(name)).hexdigest() != value:
                raise ValueError(f'Archived content checksum mismatch: {name}')
    print(json.dumps({'zip': str(output), 'bytes': output.stat().st_size, 'sha256': digest(output), 'files': len(files), 'archetypes': len(manifest['models'])}, indent=2))


if __name__ == '__main__':
    main()
