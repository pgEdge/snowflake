"""Validation harness only (never part of PEP): the complete package-owned manifest of every
package file a package job staged, bound to that file's sha256.

Usage: python3 validation_manifest.py <rpm|deb> <cell_id> <package_dir> <out.json>

For each regular file in <package_dir> (the exact files the job downloaded and checksum-verified
for its receipt) it records the file name, its sha256 (computed here from the same bytes), the
package name and version, and every entry the package owns, typed as dir, file or link (with the
link target):
  * RPM: rpm -qp --qf over FILENAMES, FILEMODES and FILELINKTOS (the same list `rpm -ql` prints
    once installed, owned directories and /usr/lib/.build-id links included); a source RPM (no
    SOURCERPM of its own) lists its spec and sources by bare name, without a directory;
  * DEB: dpkg-deb -c over the data archive (the same entries `dpkg -L` prints once installed,
    with "./" as "/."); hard links are typed "hardlink".
Nothing is installed and nothing leaves the runner except this JSON. A tool failure or an
unparsable line fails the step instead of producing a partial manifest.
"""
import hashlib
import json
import os
import stat
import subprocess
import sys

SEP = "\x1f"


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def run(argv):
    p = subprocess.run(argv, capture_output=True, text=True, timeout=300)
    if p.returncode != 0:
        raise SystemExit(f"{argv[0]} failed ({p.returncode}) on {argv[-1]}: {p.stderr.strip()[:500]}")
    return p.stdout


def rpm_entries(path):
    name_ver = run(["rpm", "-qp", "--nosignature", "--nodigest", "--qf",
                    "%{NAME}" + SEP + "%{VERSION}-%{RELEASE}" + SEP + "%{SOURCERPM}", path]).strip()
    name, version, source_rpm = name_ver.split(SEP)
    source = source_rpm == "(none)"
    raw = run(["rpm", "-qp", "--nosignature", "--nodigest", "--qf",
               "[%{FILENAMES}" + SEP + "%{FILEMODES}" + SEP + "%{FILELINKTOS}\n]", path])
    entries = []
    for line in raw.splitlines():
        if not line:
            continue
        parts = line.split(SEP)
        placed = (parts[0] != "" and "/" not in parts[0]) if source else parts[0].startswith("/")
        if len(parts) != 3 or not placed or not parts[1].lstrip("-").isdigit():
            raise SystemExit(f"unparsable rpm file line in {path}: {line!r}")
        mode = int(parts[1]) & 0xFFFF
        kind = ("dir" if stat.S_ISDIR(mode) else "link" if stat.S_ISLNK(mode)
                else "file" if stat.S_ISREG(mode) else "other")
        entry = {"path": parts[0], "type": kind}
        if kind == "link":
            entry["target"] = parts[2]
        entries.append(entry)
    return name, version, entries


def deb_path(member):
    if member in ("./", "."):
        return "/."
    p = member[1:] if member.startswith("./") else member
    p = "/" + p.lstrip("/")
    return p.rstrip("/") if p != "/" else "/."


def deb_entries(path):
    fields = run(["dpkg-deb", "--showformat=${Package}" + SEP + "${Version}", "--show", path]).strip()
    name, version = fields.split(SEP)
    entries = []
    for line in run(["dpkg-deb", "-c", path]).splitlines():
        if not line.strip():
            continue
        parts = line.split(None, 5)
        if len(parts) != 6 or parts[0][0] not in "-dlhcbps":
            raise SystemExit(f"unparsable dpkg-deb line in {path}: {line!r}")
        perms, rest = parts[0], parts[5]
        kind = {"d": "dir", "-": "file", "l": "link", "h": "hardlink"}.get(perms[0], "other")
        target = None
        if kind == "link":
            if " -> " not in rest:
                raise SystemExit(f"symlink without target in {path}: {line!r}")
            rest, target = rest.split(" -> ", 1)
        elif kind == "hardlink" or " link to " in rest:
            kind = "hardlink"
            rest, target = rest.split(" link to ", 1)
        entry = {"path": deb_path(rest), "type": kind}
        if target is not None:
            entry["target"] = target
        entries.append(entry)
    return name, version, entries


def main():
    family, cell_id, package_dir, out = sys.argv[1:5]
    if family not in ("rpm", "deb"):
        raise SystemExit(f"family must be rpm or deb, got {family!r}")
    files = sorted(f for f in os.listdir(package_dir) if os.path.isfile(os.path.join(package_dir, f)))
    if not files:
        raise SystemExit(f"no staged package files in {package_dir}")
    packages = []
    for f in files:
        path = os.path.join(package_dir, f)
        name, version, entries = (rpm_entries if family == "rpm" else deb_entries)(path)
        if not entries:
            raise SystemExit(f"{f} lists no entries")
        packages.append({"file": f, "sha256": sha256_of(path), "package_name": name, "version": version,
                         "entry_count": len(entries), "entries": entries})
    doc = {"schema": "pep-validation-manifest/1",
           "note": "existing-package validation manifest: bound to the staged published files of this run, "
                   "not to an original release build",
           "cell_id": cell_id, "family": family, "packages": packages}
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w") as fh:
        json.dump(doc, fh, indent=1, sort_keys=True)
    print(f"manifest: {len(packages)} package file(s), "
          + ", ".join(f"{p['package_name']} {p['entry_count']} entries" for p in packages))


if __name__ == "__main__":
    main()
