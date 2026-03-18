#!/usr/bin/env python3
"""
Music Library Organizer
========================
Recursively scans a music folder, reads metadata, organises by Artist/Album,
keeps the highest-quality FLAC as the "master" copy, and moves every other
version (lower-quality FLACs, MP3s, Opus, OGG, AAC, WAV, etc.) into:
 
    <DEST>/<Artist>/<Album>/
    <DEST>/<Artist>/Duplicates/<FORMAT>/
 
Usage:
    python music_organizer.py --source /path/to/FLAC --dest /path/to/output
    python music_organizer.py --source /path/to/FLAC --dest /path/to/output --dry-run
    python music_organizer.py --source /path/to/FLAC --dest /path/to/output --move
"""
 
import argparse
import hashlib
import logging
import os
import re
import shutil
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path
 
# ──────────────────────────────────────────────
# Dependency bootstrap
# ──────────────────────────────────────────────
def _ensure(pkg, import_as=None):
    import importlib
    name = import_as or pkg
    try:
        return importlib.import_module(name)
    except ImportError:
        print(f"[setup] Installing {pkg} …")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"])
        return importlib.import_module(name)
 
mutagen = _ensure("mutagen")
rich    = _ensure("rich")
 
from mutagen import File as MutagenFile          # noqa: E402
from mutagen.flac import FLAC                    # noqa: E402
from rich.console import Console                 # noqa: E402
from rich.progress import (Progress, SpinnerColumn,  # noqa: E402
                            BarColumn, TextColumn, TimeElapsedColumn)
from rich.table import Table                     # noqa: E402
from rich import print as rprint                 # noqa: E402
 
console = Console()
 
# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────
MUSIC_EXTS = {".flac", ".mp3", ".opus", ".ogg", ".m4a", ".aac",
              ".wav", ".aiff", ".wv", ".ape", ".wma", ".alac"}
 
UNKNOWN_ARTIST = "Unknown Artist"
UNKNOWN_ALBUM  = "Unknown Album"
UNKNOWN_TITLE  = "Unknown Title"
 
logging.basicConfig(
    filename="music_organizer.log",
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)
 
 
# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _safe_str(value) -> str:
    """Extract the first string value from a mutagen tag list."""
    if value is None:
        return ""
    if isinstance(value, list):
        value = value[0] if value else ""
    return str(value).strip()
 
 
def _normalize_key(text: str) -> str:
    """Lowercase, strip accents, remove punctuation → stable dict key."""
    text = unicodedata.normalize("NFKD", str(text))
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.lower()
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text
 
 
def _safe_filename(name: str, max_len: int = 100) -> str:
    """Strip characters that are illegal in filenames on Win/Mac/Linux."""
    name = unicodedata.normalize("NFKC", str(name)).strip()
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    name = name.strip(". ")
    return name[:max_len] or "_"
 
 
def _file_md5(path: Path, chunk: int = 1 << 20) -> str:
    """MD5 of raw file bytes (used to catch exact bit-for-bit duplicates)."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            data = f.read(chunk)
            if not data:
                break
            h.update(data)
    return h.hexdigest()
 
 
# ──────────────────────────────────────────────
# Metadata extraction
# ──────────────────────────────────────────────
class TrackInfo:
    __slots__ = (
        "path", "ext", "artist", "album_artist", "album",
        "title", "track_num", "year",
        "sample_rate", "bits_per_sample", "channels",
        "bitrate_kbps", "duration_sec",
        "quality_score", "md5",
    )
 
    def __repr__(self):
        return (f"<Track {self.path.name!r} "
                f"| {self.artist} – {self.title} "
                f"| Q={self.quality_score}>")
 
 
def _quality_score(info: TrackInfo) -> float:
    """
    Higher is better.  Scoring priority:
      1. FLAC vs everything else  (+1 000 000)
      2. Bits per sample          (×10 000)
      3. Sample rate kHz          (×1)
      4. Bitrate kbps             (×0.01)
    """
    score = 0.0
    if info.ext == ".flac":
        score += 1_000_000
    bps = info.bits_per_sample or 16
    sr  = (info.sample_rate or 44100) / 1000
    br  = info.bitrate_kbps or 0
    score += bps * 10_000 + sr + br * 0.01
    return score
 
 
def read_track(path: Path) -> TrackInfo | None:
    """Read audio metadata; return None if file is not a recognised audio file."""
    ext = path.suffix.lower()
    if ext not in MUSIC_EXTS:
        return None
    try:
        audio = MutagenFile(path, easy=False)
        if audio is None:
            return None
    except Exception as exc:
        log.warning("Cannot read %s: %s", path, exc)
        return None
 
    t = TrackInfo()
    t.path = path
    t.ext  = ext
 
    tags = audio.tags or {}
 
    def tag(*keys):
        for k in keys:
            try:
                v = tags.get(k)
                if v:
                    return _safe_str(v)
            except (ValueError, KeyError):
                continue
        return ""
 
    # ── common tag names across formats ──────
    t.artist       = (tag("TPE1", "TPE2", "ARTIST", "©ART", "author")
                      or UNKNOWN_ARTIST)
    t.album_artist = tag("TPE2", "ALBUMARTIST", "album_artist") or t.artist
    t.album        = tag("TALB", "ALBUM", "©alb")      or UNKNOWN_ALBUM
    t.title        = tag("TIT2", "TITLE", "©nam")      or path.stem
    t.year         = tag("TDRC", "TYER", "DATE", "©day")
 
    try:
        tn = tag("TRCK", "TRACKNUMBER", "trkn")
        t.track_num = int(str(tn).split("/")[0])
    except (ValueError, TypeError):
        t.track_num = 0
 
    # ── audio stream info ──────────────────────
    info = getattr(audio, "info", None)
    t.sample_rate    = getattr(info, "sample_rate",    None)
    t.bits_per_sample= getattr(info, "bits_per_sample", None)
    t.channels       = getattr(info, "channels",       None)
    t.bitrate_kbps   = int(getattr(info, "bitrate", 0) / 1000) if info else 0
    t.duration_sec   = getattr(info, "length", 0.0)
 
    # For FLAC specifically, pull from FLAC object for accuracy
    if ext == ".flac":
        try:
            flac = FLAC(path)
            t.sample_rate     = flac.info.sample_rate
            t.bits_per_sample = flac.info.bits_per_sample
            t.channels        = flac.info.channels
            t.bitrate_kbps    = int(flac.info.total_samples *
                                    flac.info.bits_per_sample *
                                    flac.info.channels /
                                    max(flac.info.length, 0.001) / 1000)
        except Exception:
            pass
 
    t.quality_score = _quality_score(t)
    t.md5 = None   # computed lazily only when needed
    return t
 
 
# ──────────────────────────────────────────────
# Scanning
# ──────────────────────────────────────────────
def scan_folder(source: Path) -> list[TrackInfo]:
    all_files = [p for p in source.rglob("*")
                 if p.is_file() and p.suffix.lower() in MUSIC_EXTS]
 
    tracks: list[TrackInfo] = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Scanning files…", total=len(all_files))
        for path in all_files:
            ti = read_track(path)
            if ti:
                tracks.append(ti)
            else:
                log.warning("Skipped (unreadable): %s", path)
            progress.advance(task)
 
    console.print(f"[green]✓[/green] Found [bold]{len(tracks)}[/bold] music files.")
    return tracks
 
 
# ──────────────────────────────────────────────
# Deduplication logic
# ──────────────────────────────────────────────
def group_tracks(tracks: list[TrackInfo]):
    """
    Returns a dict:
        (artist_key, title_key) → [TrackInfo, …]   sorted best→worst quality
    """
    groups: dict[tuple, list[TrackInfo]] = defaultdict(list)
    for t in tracks:
        a_key = _normalize_key(t.artist)
        ti_key = _normalize_key(t.title)
        groups[(a_key, ti_key)].append(t)
 
    # Sort each group best → worst
    for key in groups:
        groups[key].sort(key=lambda x: x.quality_score, reverse=True)
 
    return groups
 
 
def resolve_exact_duplicates(group: list[TrackInfo]) -> list[TrackInfo]:
    """
    Within a group already sorted best→worst, also collapse files that are
    bit-for-bit identical (same MD5) keeping only one representative.
    """
    seen_md5: set[str] = set()
    deduped: list[TrackInfo] = []
    for t in group:
        if t.md5 is None:
            t.md5 = _file_md5(t.path)
        if t.md5 not in seen_md5:
            seen_md5.add(t.md5)
            deduped.append(t)
        else:
            log.info("Exact duplicate (same MD5 ignored): %s", t.path)
    return deduped
 
 
# ──────────────────────────────────────────────
# Output path construction
# ──────────────────────────────────────────────
def master_dest(dest_root: Path, t: TrackInfo) -> Path:
    """The 'keeper' location: DEST/<Artist>/<Album>/filename.ext"""
    artist_dir = dest_root / _safe_filename(t.artist)
    album_dir  = artist_dir / _safe_filename(t.album)
    return album_dir / _safe_filename(t.path.name)
 
 
def duplicate_dest(dest_root: Path, t: TrackInfo) -> Path:
    """
    Duplicate location:
        DEST/<Artist>/Duplicates/<FORMAT_UPPER>/<filename>
    """
    fmt = t.ext.lstrip(".").upper()   # e.g. FLAC, MP3, OPUS
    dup_dir = dest_root / _safe_filename(t.artist) / "Duplicates" / fmt
    return dup_dir / _safe_filename(t.path.name)
 
 
def _unique_path(path: Path) -> Path:
    """Append _1, _2 … to stem if the target already exists."""
    if not path.exists():
        return path
    stem, suffix = path.stem, path.suffix
    parent = path.parent
    counter = 1
    while True:
        candidate = parent / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1
 
 
# ──────────────────────────────────────────────
# File operations
# ──────────────────────────────────────────────
def transfer(src: Path, dst: Path, move: bool, dry_run: bool) -> None:
    dst = _unique_path(dst)
    verb = "MOVE" if move else "COPY"
    if dry_run:
        console.print(f"  [dim]{verb}[/dim] {src.name} → [cyan]{dst.parent}[/cyan]")
        log.info("[DRY] %s %s → %s", verb, src, dst)
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        if move:
            shutil.move(str(src), dst)
        else:
            shutil.copy2(src, dst)
        log.info("%s %s → %s", verb, src, dst)
    except Exception as exc:
        console.print(f"  [red]ERROR[/red] {src.name}: {exc}")
        log.error("Failed to %s %s → %s: %s", verb, src, dst, exc)
 
 
# ──────────────────────────────────────────────
# Main organiser
# ──────────────────────────────────────────────
def organise(source: Path, dest: Path, move: bool, dry_run: bool,
             skip_md5: bool) -> None:
 
    if dry_run:
        console.print("[yellow]⚠  DRY RUN — no files will be touched.[/yellow]\n")
 
    # 1. Scan
    tracks = scan_folder(source)
    if not tracks:
        console.print("[red]No music files found.[/red]")
        return
 
    # 2. Group by (artist, title)
    groups = group_tracks(tracks)
    console.print(f"[green]✓[/green] [bold]{len(groups)}[/bold] unique tracks identified "
                  f"across [bold]{len(tracks)}[/bold] files.\n")
 
    # 3. Decide keeper vs duplicates
    masters:    list[tuple[TrackInfo, Path]] = []
    duplicates: list[tuple[TrackInfo, Path]] = []
 
    total_groups = len(groups)
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    ) as prog:
        task = prog.add_task("Resolving duplicates…", total=total_groups)
 
        for group_list in groups.values():
            if not skip_md5 and len(group_list) > 1:
                group_list = resolve_exact_duplicates(group_list)
 
            keeper = group_list[0]
            masters.append((keeper, master_dest(dest, keeper)))
 
            for dup in group_list[1:]:
                duplicates.append((dup, duplicate_dest(dest, dup)))
 
            prog.advance(task)
 
    # 4. Summary table
    table = Table(title="Organisation Summary", show_lines=True)
    table.add_column("Category",   style="bold")
    table.add_column("Count",      justify="right")
    table.add_row("[green]Masters (keep)[/green]",     str(len(masters)))
    table.add_row("[yellow]Duplicates (move to /Duplicates)[/yellow]", str(len(duplicates)))
    console.print(table)
    console.print()
 
    # 5. Transfer masters
    console.rule("[bold green]Transferring masters")
    for t, dst in masters:
        transfer(t.path, dst, move=move, dry_run=dry_run)
 
    # 6. Transfer duplicates
    if duplicates:
        console.rule("[bold yellow]Transferring duplicates")
        for t, dst in duplicates:
            transfer(t.path, dst, move=move, dry_run=dry_run)
 
    console.print()
    console.print(f"[bold green]✓ Done![/bold green]  "
                  f"Masters: {len(masters)}  |  Duplicates: {len(duplicates)}")
    if not dry_run:
        console.print(f"[dim]Full log: music_organizer.log[/dim]")
 
 
# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Organise a music library by artist/album, "
                    "deduplicating and keeping the highest-quality FLAC.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
  # Safe preview — nothing is touched:
  python music_organizer.py --source ~/Music/FLAC --dest ~/Music/Organised --dry-run
 
  # Copy files (source folder unchanged):
  python music_organizer.py --source ~/Music/FLAC --dest ~/Music/Organised
 
  # Move files (source folder is emptied):
  python music_organizer.py --source ~/Music/FLAC --dest ~/Music/Organised --move
 
Output structure
----------------
  <DEST>/
    Radiohead/
      OK Computer/
        01 - Airbag.flac           ← highest-quality master
      Duplicates/
        FLAC/
          Airbag_128k.flac         ← lower-bitrate FLAC duplicate
        MP3/
          Airbag.mp3               ← any MP3 version
        OPUS/
          Airbag.opus              ← any Opus version
""",
    )
    parser.add_argument("--source", required=True,
                        help="Root folder to scan recursively")
    parser.add_argument("--dest",   required=True,
                        help="Output folder (will be created if needed)")
    parser.add_argument("--move",   action="store_true",
                        help="Move files instead of copying (destructive!)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print planned actions without touching files")
    parser.add_argument("--skip-md5", action="store_true",
                        help="Skip MD5 check for exact duplicates (faster)")
 
    args = parser.parse_args()
 
    source = Path(args.source).expanduser().resolve()
    dest   = Path(args.dest).expanduser().resolve()
 
    if not source.is_dir():
        console.print(f"[red]Source folder not found:[/red] {source}")
        sys.exit(1)
 
    if dest == source:
        console.print("[red]Source and destination must be different folders.[/red]")
        sys.exit(1)
 
    console.print(f"\n[bold]Music Library Organiser[/bold]")
    console.print(f"  Source : [cyan]{source}[/cyan]")
    console.print(f"  Dest   : [cyan]{dest}[/cyan]")
    console.print(f"  Mode   : {'[red]MOVE[/red]' if args.move else '[green]COPY[/green]'}")
    console.print(f"  MD5    : {'[dim]skipped[/dim]' if args.skip_md5 else 'enabled'}\n")
 
    if args.move and not args.dry_run:
        console.print("[bold red]WARNING:[/bold red] --move will permanently relocate "
                      "files from the source folder.\n"
                      "Press [bold]Ctrl+C[/bold] to abort, or wait 5 seconds to continue…")
        import time
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            console.print("\n[yellow]Aborted.[/yellow]")
            sys.exit(0)
 
    organise(source, dest, move=args.move,
             dry_run=args.dry_run, skip_md5=args.skip_md5)
 
 
if __name__ == "__main__":
    main()
