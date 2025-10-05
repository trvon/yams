// Tiny runtime check for SQLite FTS5
#include <sqlite3.h>
int main() {
    int enabled = sqlite3_compileoption_used("ENABLE_FTS5");
    return enabled ? 0 : 1;
}
