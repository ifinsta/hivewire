#!/usr/bin/env python3
"""Deterministic Git-anchored source impact gate for Hivewire (Python 3.11+).

Advisory lexical analysis, not an AI review or compiler-resolved call graph.
Must run as one of Hivewire's *independently configured* executable gates.
The merge gate and outcome tests remain the authoritative acceptance path.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

MAX_FILES = 12000
MAX_BLOB = 1_000_000
MAX_OUTPUT = 1000
SOURCE = {'.rs', '.py', '.ts', '.tsx', '.js', '.jsx', '.go', '.java', '.c', '.cpp', '.h'}
CONTRACT = {'.proto', '.graphql', '.gql', '.sql'}
PUBLIC = {
    '.rs': re.compile(r'(?m)^\s*pub(?:\([^)]*\))?\s+(?:(?:async|unsafe)\s+)*(?:fn|struct|enum|trait|type|mod|const)\s+([A-Za-z_]\w*)'),
    '.py': re.compile(r'(?m)^(?:def|class)\s+([A-Za-z_]\w*)'),
    '.go': re.compile(r'(?m)^func\s+(?:\([^)]*\)\s*)?([A-Z]\w*)'),
}
SCRIPT_EXPORT = re.compile(r'(?m)^\s*export\s+(?:(?:default|declare|async)\s+)*(?:(?:function|class|const|interface|type|enum)\s+)([A-Za-z_]\w*)')
IMPORT = [
    re.compile(r'(?m)^\s*(?:pub\s+)?(?:use|mod)\s+(?:(?:crate|super|self)::)?([A-Za-z_]\w*(?:::\w+)*)'),
    re.compile(r'(?m)^\s*(?:from|import)\s+([A-Za-z_]\w*(?:\.\w+)*)'),
    re.compile(r'''(?m)\b(?:from\s*|require\s*\(\s*|import\s*\(\s*)['"](?:\.?\.?/)?([A-Za-z_]\w*(?:[/.-]\w+)*)'''),
]

class GateError(Exception):
    pass


def git(root: Path, *args: str, byte_limit: int = 4_000_000) -> bytes:
    try:
        proc = subprocess.run(['git', *args], cwd=root, capture_output=True, timeout=25, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise GateError(f'git unavailable or timed out: {type(exc).__name__}') from exc
    if proc.returncode:
        raise GateError(f'git {args[0]} failed (exit {proc.returncode})')
    if len(proc.stdout) > byte_limit:
        raise GateError('git result exceeds scan budget')
    return proc.stdout


def resolve(root: Path, revision: str) -> str:
    result = git(root, 'rev-parse', '--verify', f'{revision}^{{commit}}').decode('ascii').strip()
    if not re.fullmatch('[0-9a-f]{40,64}', result):
        raise GateError('invalid commit identifier')
    return result


def tree(root: Path, sha: str) -> dict[str, tuple[str, int]]:
    raw = git(root, 'ls-tree', '-rzl', sha, byte_limit=8_000_000)
    entries = raw.split(b'\0')
    if len(entries) > MAX_FILES * 3:
        raise GateError('repository tree exceeds scan budget')
    result: dict[str, tuple[str, int]] = {}
    for item in entries:
        if not item:
            continue
        try:
            meta, name = item.split(b'\t', 1)
            mode, kind, blob, size = meta.decode('ascii').split()
            path = name.decode('utf-8')
            if '\x00' in path or not path or len(path) > 512:
                raise ValueError('unsafe path')
            if mode == '120000' or kind != 'blob' or not size.isdigit():
                continue
            if int(size) <= MAX_BLOB and Path(path).suffix.lower() in (SOURCE | CONTRACT):
                result[path] = (blob, int(size))
        except (UnicodeError, ValueError) as exc:
            raise GateError('unparseable tree entry; manual review required') from exc
        if len(result) > MAX_FILES:
            raise GateError('eligible source count exceeds scan budget')
    return result


def blob(root: Path, sha: str) -> str:
    if not re.fullmatch('[0-9a-f]{40,64}', sha):
        raise GateError('invalid Git blob id')
    return git(root, 'cat-file', 'blob', sha, byte_limit=MAX_BLOB).decode('utf-8', errors='replace')


def symbols(text: str, ext: str) -> set[str]:
    pattern = PUBLIC.get(ext, SCRIPT_EXPORT if ext in {'.ts', '.tsx', '.js', '.jsx'} else None)
    if pattern is None:
        return set()
    return {name for name in pattern.findall(text) if not name.startswith('_')}


def dependencies(text: str) -> set[str]:
    result: set[str] = set()
    for pattern in IMPORT:
        for hit in pattern.findall(text):
            result.update(token.casefold() for token in re.split(r'[:./-]+', hit) if len(token) > 1)
            if len(result) >= 256:
                return set(sorted(result)[:256])
    return result


def changed(root: Path, base: str, head: str) -> dict[str, str]:
    raw = git(root, 'diff', '--no-renames', '--name-status', '-z', base, head)
    parts = raw.split(b'\0')
    if len(parts) > MAX_FILES * 2 + 5:
        raise GateError('diff exceeds scan budget')
    if parts[-1:] == [b'']:
        parts.pop()
    if len(parts) % 2:
        raise GateError('malformed Git name-status diff')
    try:
        return {parts[n + 1].decode('utf-8'): parts[n].decode('ascii') for n in range(0, len(parts), 2)}
    except UnicodeError as exc:
        raise GateError('non-UTF8 paths require independent review') from exc


def inspect(root: Path, base: str, head: str, policy: dict | None = None) -> dict:
    ancestor = subprocess.run(['git', 'merge-base', '--is-ancestor', base, head], cwd=root, timeout=25, check=False)
    if ancestor.returncode != 0:
        raise GateError('base must be an ancestor of head')
    before, after = tree(root, base), tree(root, head)
    changes = changed(root, base, head)
    consumers = {}
    # Read the pinned final commit, not any potentially dirty working tree.
    for path, (oid, _) in after.items():
        if Path(path).suffix.lower() in SOURCE:
            consumers[path] = dependencies(blob(root, oid))
    policy = policy or {}
    if not isinstance(policy, dict) or set(policy) - {'version', 'contracts'} or policy.get('version', 1) != 1:
        raise GateError('unsupported policy version/keys')
    contracts = policy.get('contracts', [])
    if not isinstance(contracts, list) or len(contracts) > 200:
        raise GateError('invalid contract policy')
    configured = {}
    for entry in contracts:
        if not isinstance(entry, dict) or set(entry) != {'path', 'consumers'}:
            raise GateError('contract requires path and consumers')
        p, refs = entry['path'], entry['consumers']
        if not isinstance(p, str) or p.startswith('/') or '..' in Path(p).parts or not isinstance(refs, list) or len(refs) > 50 or any(not isinstance(x, str) or not re.fullmatch(r'[\w.-]+/[\w.-]+', x) for x in refs):
            raise GateError('invalid contract path or consumers')
        configured[p] = sorted(set(refs))
    findings = []
    for path, status in sorted(changes.items()):
        ext = Path(path).suffix.lower()
        if ext not in (SOURCE | CONTRACT) and path not in configured:
            continue
        missing_required_blob = (status.startswith('M') and (path not in before or path not in after))
        if missing_required_blob or (path not in before and path not in after):
            findings.append({'path': path, 'status': status, 'severity': 'block',
                             'removedSymbols': [], 'affectedPaths': [], 'externalConsumers': [],
                             'reasons': ['unindexed_or_unsafe_change']})
            continue
        old = blob(root, before[path][0]) if path in before else ''
        new = blob(root, after[path][0]) if path in after else ''
        removed = sorted(symbols(old, ext) - symbols(new, ext))
        token = Path(path).stem.casefold()
        affected = sorted(p for p, deps in consumers.items() if p != path and token in deps)[:MAX_OUTPUT]
        external = configured.get(path, [])
        severity = 'block' if removed or ext in CONTRACT or external else 'informational'
        reasons = []
        if removed:
            reasons.append('removed_public_symbols')
        if ext in CONTRACT:
            reasons.append('unverified_contract_change')
        if external:
            reasons.append('declared_external_consumers_not_tested')
        if affected:
            reasons.append('lexical_reverse_dependencies')
        findings.append({'path': path, 'status': status, 'severity': severity,
                         'removedSymbols': removed[:MAX_OUTPUT], 'affectedPaths': affected,
                         'externalConsumers': external, 'reasons': reasons})
    blocked = [f for f in findings if f['severity'] == 'block']
    payload = {'schemaVersion': 1, 'base': base, 'head': head, 'scannedFiles': len(consumers),
               'changedFiles': len(changes), 'findings': findings, 'blockingCount': len(blocked),
               'semanticCompleteness': False, 'independentApproval': False,
               'result': 'blocking_static_findings' if blocked else 'no_blocking_static_findings',
               'limitations': ['lexical references only', 'no runtime execution', 'no independent AI review',
                               'unindexed or dynamic consumers may be missed']}
    payload['evidenceSha256'] = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return payload


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', default='.', help='Repository checkout (no network fetch)')
    parser.add_argument('--base', default='HEAD^', help='Git base commit/ref')
    parser.add_argument('--head', default='HEAD', help='Git head commit/ref')
    parser.add_argument('--policy', help='Trusted policy JSON file outside the reviewed worktree')
    args = parser.parse_args(argv)
    try:
        root = Path(args.repo).resolve(strict=True)
        base, head = resolve(root, args.base), resolve(root, args.head)
        policy = None
        if args.policy:
            p = Path(args.policy).resolve(strict=True)
            if root == p or root in p.parents:
                raise GateError('policy must be independent of the reviewed working tree')
            if p.stat().st_size > 100_000:
                raise GateError('policy exceeds limit')
            policy = json.loads(p.read_text(encoding='utf-8'))
        result = inspect(root, base, head, policy)
        print(json.dumps(result, sort_keys=True))
        return 2 if result['blockingCount'] else 0
    except (GateError, OSError, ValueError, subprocess.TimeoutExpired, json.JSONDecodeError) as exc:
        print(json.dumps({'result': 'unknown', 'error': str(exc), 'independentApproval': False}), file=sys.stderr)
        return 3

if __name__ == '__main__':
    sys.exit(main())
