#!/usr/bin/env python3
"""
License Header Updater Script
Replaces Apache-2.0 and MIT license headers with GPL-3.0-or-later
"""

import os
import re
import sys
from pathlib import Path

# Directories to exclude (third-party code)
EXCLUDE_DIRS = {
    'third_party',
    'subprojects',
    'external',
    '.git',
    'build',
    'builddir',
    'node_modules',
    '.conan2',
}

# File extensions to check
SOURCE_EXTENSIONS = {
    '.cpp', '.h', '.hpp', '.c', '.cc', '.cxx',
    '.py', '.ts', '.js', '.sh', '.ps1', '.bash',
}

# License replacements
LICENSE_REPLACEMENTS = [
    # SPDX short form
    (r'SPDX-License-Identifier:\s*Apache-2\.0', 'SPDX-License-Identifier: GPL-3.0-or-later'),
    (r'SPDX-License-Identifier:\s*MIT', 'SPDX-License-Identifier: GPL-3.0-or-later'),
    (r'SPDX-License-Identifier:\s*BSD[^\n]*', 'SPDX-License-Identifier: GPL-3.0-or-later'),
    # Full Apache license text (multi-line)
    (r'Licensed under the Apache License, Version 2\.0.*?limitations under the License\.',
     'This program is free software: you can redistribute it and/or modify\n// it under the terms of the GNU General Public License as published by\n// the Free Software Foundation, either version 3 of the License, or\n// (at your option) any later version.\n//\n// This program is distributed in the hope that it will be useful,\n// but WITHOUT ANY WARRANTY; without even the implied warranty of\n// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the\n// GNU General Public License for more details.\n//\n// You should have received a copy of the GNU General Public License\n// along with this program.  If not, see <https://www.gnu.org/licenses/>.'),
    # BSD-3-Clause license text (multi-line)
    (r'Redistribution and use in source and binary forms.*?OR TORT.*?\n.*?\(INCLUDING NEGLIGENCE OR OTHERWISE\).*?ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE.*?EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE\.',
     'This program is free software: you can redistribute it and/or modify\n// it under the terms of the GNU General Public License as published by\n// the Free Software Foundation, either version 3 of the License, or\n// (at your option) any later version.\n//\n// This program is distributed in the hope that it will be useful,\n// but WITHOUT ANY WARRANTY; without even the implied warranty of\n// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the\n// GNU General Public License for more details.\n//\n// You should have received a copy of the GNU General Public License\n// along with this program.  If not, see <https://www.gnu.org/licenses/>.'),
]

def should_exclude(path):
    """Check if path should be excluded"""
    parts = Path(path).parts
    for part in parts:
        if part in EXCLUDE_DIRS:
            return True
    return False

def update_file(filepath):
    """Update license headers in a single file"""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return False
    
    original_content = content
    changes_made = []
    
    for pattern, replacement in LICENSE_REPLACEMENTS:
        if re.search(pattern, content, re.IGNORECASE | re.DOTALL):
            content = re.sub(pattern, replacement, content, flags=re.IGNORECASE | re.DOTALL)
            changes_made.append(f"  Replaced: {pattern[:40]}...")
    
    if content != original_content:
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Updated: {filepath}")
            for change in changes_made:
                print(change)
            return True
        except Exception as e:
            print(f"Error writing {filepath}: {e}")
            return False
    
    return False

def find_and_update_files(root_dir):
    """Find and update all source files"""
    updated_count = 0
    checked_count = 0
    
    for root, dirs, files in os.walk(root_dir):
        # Remove excluded directories from traversal
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
        
        for file in files:
            ext = Path(file).suffix.lower()
            if ext not in SOURCE_EXTENSIONS:
                continue
            
            filepath = os.path.join(root, file)
            if should_exclude(filepath):
                continue
            
            checked_count += 1
            if update_file(filepath):
                updated_count += 1
    
    return checked_count, updated_count

def main():
    if len(sys.argv) > 1:
        root_dir = sys.argv[1]
    else:
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    print(f"Scanning for license headers in: {root_dir}")
    print(f"Excluding directories: {', '.join(EXCLUDE_DIRS)}")
    print("-" * 60)
    
    checked, updated = find_and_update_files(root_dir)
    
    print("-" * 60)
    print(f"Checked {checked} files")
    print(f"Updated {updated} files")
    
    return 0 if updated > 0 else 1

if __name__ == '__main__':
    sys.exit(main())
