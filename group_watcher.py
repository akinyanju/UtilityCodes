#To do
#if group not found, dont add user and dont remove user.
#group can be added with --add group function. and can also be removed
#Full path dont need to be specificed input and --out files are in current director
#EricWang_Lab_CT

import argparse
import json
import os
import sys
import time
import csv
import subprocess
from glob import glob
from threading import Thread
import textwrap

# Auto-install watchdog if needed
try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "watchdog"])
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler

# Constants
WATCHER_STATE_FILE = ".group_watcher_state.json"
LOG_FILE = "group_watcher.log"

# -------- Logging --------
def log(message):
    timestamp = time.strftime("[%Y-%m-%d %H:%M:%S]")
    with open(LOG_FILE, "a") as f:
        f.write(f"{timestamp} {message}\n")

# -------- JSON Profile Handling --------
def load_profile(profile_file):
    if os.path.exists(profile_file):
        with open(profile_file, 'r') as f:
            return json.load(f)
    return {}

def save_profile(profile, profile_file):
    os.makedirs(os.path.dirname(profile_file), exist_ok=True)
    with open(profile_file, 'w') as f:
        json.dump(profile, f, indent=4)
    generate_html_view(profile, profile_file)

# -------- HTML View Generator --------
def generate_html_view(profile, profile_file):
    html_file = os.path.splitext(profile_file)[0] + ".html"
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>User Groups</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            table { width: 100%; border-collapse: collapse; }
            th, td { padding: 8px 12px; border: 1px solid #ccc; }
            th { background-color: #f2f2f2; }
        </style>
    </head>
    <body>
        <h1>User Groups</h1>
        <table>
            <thead><tr><th>Group</th><th>Users</th></tr></thead>
            <tbody>
    """

    for group, users in profile.items():
        html += f"<tr><td>{group}</td><td>{', '.join(users)}</td></tr>"

    html += """
            </tbody>
        </table>
    </body>
    </html>
    """

    with open(html_file, "w") as f:
        f.write(html)

    log(f"Generated HTML view: {html_file}")

# -------- Extract Groups from Files --------
def extract_groups_from_files(base_path):
    groups = set()
    pattern = os.path.join(base_path, "**", "*.metrics.txt")
    for file in glob(pattern, recursive=True):
        try:
            with open(file, 'r', newline='') as f:
                sample = f.read(2048)
                f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters='\t,')
                except csv.Error:
                    log(f"Could not sniff delimiter for file {file}, defaulting to tab.")
                    dialect = csv.excel_tab

                reader = csv.DictReader(f, dialect=dialect)

                if 'Investigator_Folder' in reader.fieldnames:
                    for row in reader:
                        folder = row.get('Investigator_Folder')
                        if folder:
                            groups.add(folder.strip())
                else:
                    log(f"'Investigator_Folder' column not found in file: {file}")

        except Exception as e:
            log(f"Error reading file {file}: {e}")
    return groups

# -------- Update JSON Profile with Groups --------
def update_profile_with_groups(groups, profile_file):
    profile = load_profile(profile_file)
    for group in groups:
        if group and group != 'Investigator_Folder' and group not in profile:
            profile[group] = []
    save_profile(profile, profile_file)
    log(f"Updated profile with groups: {groups}")

# -------- Add / Remove Users --------
def add_users_to_group(profile_file, group, emails):
    profile = load_profile(profile_file)
    profile.setdefault(group, [])
    for email in emails:
        if email not in profile[group]:
            profile[group].append(email)
    save_profile(profile, profile_file)
    log(f"Added users {emails} to group {group}")

def remove_users_from_group(profile_file, group, emails):
    profile = load_profile(profile_file)
    if group in profile:
        profile[group] = [email for email in profile[group] if email not in emails]
        save_profile(profile, profile_file)
        log(f"Removed users {emails} from group {group}")

# -------- Watchdog Handlers --------
class MetricsFileHandler(FileSystemEventHandler):
    def __init__(self, input_dir, profile_file):
        self.input_dir = input_dir
        self.profile_file = profile_file

    def on_modified(self, event):
        if event.src_path.endswith(".metrics.txt"):
            log(f"Detected change: {event.src_path}")
            groups = extract_groups_from_files(self.input_dir)
            update_profile_with_groups(groups, self.profile_file)

def start_watcher(input_dir, profile_file):
    observer = Observer()
    handler = MetricsFileHandler(input_dir, profile_file)
    observer.schedule(handler, path=input_dir, recursive=True)
    observer.start()

    with open(WATCHER_STATE_FILE, 'w') as f:
        json.dump({'watching': True, 'input_dir': input_dir, 'profile_file': profile_file}, f)

    log("Watcher started.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()
        log("Watcher stopped.")

def disable_watcher():
    if os.path.exists(WATCHER_STATE_FILE):
        os.remove(WATCHER_STATE_FILE)
        log("Watcher disabled.")
    else:
        log("No watcher running.")

def auto_restart_watcher_if_needed():
    if os.path.exists(WATCHER_STATE_FILE):
        with open(WATCHER_STATE_FILE, 'r') as f:
            state = json.load(f)
            if state.get('watching'):
                log("Restarting watcher from saved state.")
                thread = Thread(target=start_watcher, args=(state['input_dir'], state['profile_file']), daemon=True)
                thread.start()

# -------- CLI --------
def main():
    parser = argparse.ArgumentParser(
        description="Group watcher for *.metrics.txt files.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=textwrap.dedent("""
            Example 1: Extract groups from metrics files and save to .usersProfile.json
                python group_watcher.py --inputDir /path/to/metrics/dir --out /path/to/output/.usersProfile.json
            Example 2: Add users to a group
                python group_watcher.py --add-users --out /path/to/output/.usersProfile.json --group GroupX --emails a@x.com,b@y.com
            Example 3: Remove users from a group
                python group_watcher.py --remove-users --out /path/to/output/.usersProfile.json --group GroupX --emails a@x.com,b@y.com
            Example 4: Start background watcher to monitor changes in *.metrics.txt files
                python group_watcher.py --out /path/to/output/.usersProfile.json --watch
            Example 5: Disable background watcher
                python group_watcher.py --out /path/to/output/.usersProfile.json --disable-watch
        """)
    )

    parser.add_argument('--inputDir', type=str, help='Directory to scan/watch for metrics files.')
    parser.add_argument('--out', type=str, help='Path to output .usersProfile.json file.')
    parser.add_argument('--group', type=str, help='Group name for assigning or removing users.')
    parser.add_argument('--emails', type=str, nargs='*', help='List of email addresses to assign/remove.')
    parser.add_argument('--email-file', type=str, help='File containing list of email addresses.')
    parser.add_argument('--add-users', action='store_true', help='Add users to group.')
    parser.add_argument('--remove-users', action='store_true', help='Remove users from group.')
    parser.add_argument('--watch', action='store_true', help='Start background watcher.')
    parser.add_argument('--disable-watch', action='store_true', help='Disable background watcher.')

    args = parser.parse_args()

    auto_restart_watcher_if_needed()

    if args.watch:
        if not args.inputDir or not args.out:
            parser.error("--inputDir and --out are required for watcher mode.")
    elif args.add_users or args.remove_users:
        if not args.group or not args.out:
            parser.error("--group and --out are required for user modification.")
    else:
        if not args.inputDir or not args.out:
            parser.error("--inputDir and --out are required for group extraction.")

    profile_file = args.out
    emails = []
    if args.emails:
        for email in args.emails:
            if ',' in email:
                emails.extend(email.split(','))
            else:
                emails.append(email)
    if args.email_file:
        with open(args.email_file, 'r') as f:
            emails += [line.strip() for line in f if line.strip()]

    if args.add_users:
        add_users_to_group(profile_file, args.group, emails)
    elif args.remove_users:
        remove_users_from_group(profile_file, args.group, emails)
    elif args.watch:
        start_watcher(args.inputDir, profile_file)
    elif args.disable_watch:
        disable_watcher()
    else:
        groups = extract_groups_from_files(args.inputDir)
        update_profile_with_groups(groups, profile_file)

if __name__ == "__main__":
    main()
