/**
 * repro_nochange_bug.cpp
 *
 * Reproduces a bug in libxsql where UPDATE on a cached_table with
 * no_shared_cache() + filter_eq() + row_populator() causes the
 * row_populator to receive SQLITE_NOCHANGE (uninitialized) values
 * for non-SET columns.
 *
 * Root cause:
 *   cached_vtab_column() calls sqlite3_vtab_nochange(ctx) and returns
 *   early without setting any result.  This tells SQLite to mark those
 *   argv slots as NOCHANGE in xUpdate.  The row_populator then reads
 *   garbage/NULL for every column NOT in the SET clause.
 *
 * Build (assuming libxsql headers are on the include path):
 *   cl /std:c++20 /EHsc /I <libxsql>/include repro_nochange_bug.cpp
 *   g++ -std=c++20 -I <libxsql>/include repro_nochange_bug.cpp -o repro -lsqlite3
 *
 * Expected output (with the bug present):
 *   row_populator: owner_id is NULL (NOCHANGE) — BUG!
 *   UPDATE failed: SQL logic error
 *
 * Expected output (after fix):
 *   row_populator: owner_id = 100
 *   After UPDATE: owner_id=100 name=UPDATED note=hello
 */

#include <xsql/database.hpp>
#include <xsql/vtable.hpp>

#include <cassert>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

// --------------------------------------------------------------------
// Data model — a simple "item" with three columns
// --------------------------------------------------------------------
struct Item {
    int64_t owner_id = 0;     // read-only key (not in SET clause)
    std::string name;         // writable
    std::string note;         // writable
};

// Global data source (simulates an external API)
static std::vector<Item> g_items = {
    {100, "alpha", "hello"},
    {100, "beta",  "world"},
    {200, "gamma", "foo"},
};

// Index for filter_eq("owner_id")
class ItemsByOwnerIterator : public xsql::RowIterator {
    std::vector<Item> rows_;
    int idx_ = -1;
public:
    explicit ItemsByOwnerIterator(int64_t owner_id) {
        for (const auto& item : g_items) {
            if (item.owner_id == owner_id)
                rows_.push_back(item);
        }
    }
    bool next() override { return ++idx_ < (int)rows_.size(); }
    bool eof()  const override { return idx_ < 0 || idx_ >= (int)rows_.size(); }

    void column(xsql::FunctionContext& ctx, int col) override {
        const auto& r = rows_[idx_];
        switch (col) {
            case 0: ctx.result_int64(r.owner_id); break;
            case 1: ctx.result_text(r.name); break;
            case 2: ctx.result_text(r.note); break;
            default: ctx.result_null(); break;
        }
    }
    int64_t rowid() const override { return idx_; }
};

// --------------------------------------------------------------------
// Table definition — mirrors the idasql pseudocode table pattern:
//   no_shared_cache + filter_eq + row_populator + column_text_rw
// --------------------------------------------------------------------
static bool g_row_populator_owner_id_was_null = false;

auto build_items_table() {
    return xsql::cached_table<Item>("items")
        .no_shared_cache()
        .estimate_rows([]() -> size_t { return g_items.size(); })
        .cache_builder([](std::vector<Item>& cache) {
            cache = g_items;
        })
        .row_populator([](Item& row, int argc, xsql::FunctionArg* argv) {
            // argv[2]=owner_id, argv[3]=name, argv[4]=note
            //
            // BUG: During UPDATE that only SETs "name", argv[2] (owner_id)
            //      and argv[4] (note) are NOCHANGE — they appear as NULL
            //      and reading them yields garbage.
            if (argc > 2) {
                if (argv[2].is_null()) {
                    // NOCHANGE value — owner_id was not in SET clause
                    printf("  row_populator: owner_id is NULL (NOCHANGE) — BUG!\n");
                    g_row_populator_owner_id_was_null = true;
                } else {
                    row.owner_id = argv[2].as_int64();
                    printf("  row_populator: owner_id = %lld\n",
                           (long long)row.owner_id);
                }
            }
            if (argc > 3 && !argv[3].is_null()) {
                const char* v = argv[3].as_c_str();
                row.name = v ? v : "";
            }
            if (argc > 4 && !argv[4].is_null()) {
                const char* v = argv[4].as_c_str();
                row.note = v ? v : "";
            }
        })
        // Column 0: owner_id (read-only)
        .column_int64("owner_id",
            [](const Item& r) -> int64_t { return r.owner_id; })
        // Column 1: name (writable)
        .column_text_rw("name",
            [](const Item& r) -> std::string { return r.name; },
            [](Item& row, xsql::FunctionArg val) -> bool {
                const char* v = val.as_c_str();
                row.name = v ? v : "";
                // Simulate a setter that needs owner_id (like
                // set_decompiler_comment needs func_addr):
                if (row.owner_id == 0) {
                    printf("  name setter: FAIL — owner_id is 0 "
                           "(lost due to NOCHANGE)\n");
                    return false;
                }
                printf("  name setter: OK — owner_id=%lld, "
                       "new name='%s'\n",
                       (long long)row.owner_id, row.name.c_str());
                // Apply to backing store
                for (auto& item : g_items) {
                    if (item.owner_id == row.owner_id &&
                        item.note == row.note) {
                        item.name = row.name;
                        break;
                    }
                }
                return true;
            })
        // Column 2: note (writable)
        .column_text_rw("note",
            [](const Item& r) -> std::string { return r.note; },
            [](Item& row, xsql::FunctionArg val) -> bool {
                const char* v = val.as_c_str();
                row.note = v ? v : "";
                return true;
            })
        // Constraint pushdown on owner_id
        .filter_eq("owner_id", [](int64_t owner) {
            return std::make_unique<ItemsByOwnerIterator>(owner);
        }, 10.0, 5.0)
        .build();
}

// --------------------------------------------------------------------
// Main — run the repro
// --------------------------------------------------------------------
int main() {
    auto def = build_items_table();

    xsql::Database db;
    db.open(":memory:");
    db.register_and_create_cached_table(def);

    // 1. Verify SELECT works
    printf("=== SELECT (should work) ===\n");
    auto result = db.query(
        "SELECT owner_id, name, note FROM items WHERE owner_id = 100");
    for (const auto& row : result) {
        printf("  owner_id=%s name=%s note=%s\n",
               row[0].c_str(), row[1].c_str(), row[2].c_str());
    }

    // 2. UPDATE — only SET "name", do NOT set owner_id or note
    //    This triggers the NOCHANGE bug:
    //      - SQLite calls xColumn for owner_id and note
    //      - cached_vtab_column sees sqlite3_vtab_nochange() == true
    //      - Returns without setting any result
    //      - xUpdate argv[2] (owner_id) is NOCHANGE / uninitialized
    //      - row_populator reads garbage for owner_id
    //      - name setter fails because row.owner_id == 0
    printf("\n=== UPDATE (triggers bug) ===\n");
    printf("  SQL: UPDATE items SET name='UPDATED'\n"
           "       WHERE owner_id=100 AND name='alpha';\n");

    g_row_populator_owner_id_was_null = false;
    auto update_result = db.query(
        "UPDATE items SET name = 'UPDATED' "
        "WHERE owner_id = 100 AND name = 'alpha'");

    if (!update_result.error.empty()) {
        printf("  UPDATE failed: %s\n", update_result.error.c_str());
    } else {
        printf("  UPDATE succeeded\n");
    }

    // 3. Show final state
    printf("\n=== Final state ===\n");
    result = db.query(
        "SELECT owner_id, name, note FROM items WHERE owner_id = 100");
    for (const auto& row : result) {
        printf("  owner_id=%s name=%s note=%s\n",
               row[0].c_str(), row[1].c_str(), row[2].c_str());
    }

    // 4. Report
    printf("\n=== Result ===\n");
    if (g_row_populator_owner_id_was_null) {
        printf("  BUG CONFIRMED: row_populator received NULL for non-SET\n"
               "  column 'owner_id' due to sqlite3_vtab_nochange() early\n"
               "  return in cached_vtab_column().\n");
        return 1;
    } else {
        printf("  OK: row_populator received correct owner_id value.\n");
        return 0;
    }
}
