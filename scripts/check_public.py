"""Audit the exact Git index before public upload. Never prints matched secret text."""
from pathlib import Path
import re
import subprocess
import sys

root=Path(__file__).resolve().parents[1]
files=subprocess.check_output(['git','diff','--cached','--name-only','--diff-filter=ACMR','-z'],cwd=root).decode('utf-8').split('\0')
if '--all-tracked' in sys.argv:
    files=subprocess.check_output(['git','ls-files','-z'],cwd=root).decode('utf-8').split('\0')
forbidden_dirs={'材料','data','runtime','.venv','node_modules','.git'}
patterns={
 'private IPv4':r'(?<![\d.])(?:10\.\d{1,3}\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3})(?![\d.])',
 'private key':r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----',
 'access token':r'(?<![A-Za-z0-9])(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{30,}|sk-[A-Za-z0-9_-]{24,})',
 'local user path':r'(?:C:|D:)\\Users\\[^\\\s]+',
}
errors=[]; scanned=0
for name in filter(None,files):
    p=Path(name)
    if forbidden_dirs.intersection(p.parts) or (p.name.startswith('.env') and p.name!='.env.example') or p.suffix.lower() in {'.pdf','.docx','.db','.sqlite','.log'}:
        errors.append((name,'forbidden path'));continue
    # Read the staged version, so unstaged changes cannot hide index contents.
    raw=subprocess.check_output(['git','show',':'+name],cwd=root)
    try: text=raw.decode('utf-8')
    except UnicodeDecodeError:
        errors.append((name,'binary file requires separate review'));continue
    for label,pattern in patterns.items():
        if re.search(pattern,text): errors.append((name,label))
    scanned+=1
for file,reason in errors: print(f'BLOCKED {file}: {reason}')
print(f'Public-source audit: {scanned} text files checked, {len(errors)} issue(s).')
sys.exit(bool(errors))
