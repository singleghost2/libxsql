/**
 * xsql/database.hpp - RAII SQLite database wrapper with query helpers
 *
 * Part of libxsql - a generic SQLite virtual table framework.
 *
 * Example usage:
 *
 *   xsql::Database db;
 *   if (!db.open(":memory:")) {
 *       fprintf(stderr, "Error: %s\n", db.last_error().c_str());
 *       return 1;
 *   }
 *
 *   db.register_table(my_table_def);
 *
 *   auto result = db.query("SELECT * FROM my_table WHERE id > 10");
 *   if (!result.ok()) {
 *       fprintf(stderr, "Query error: %s\n", result.error.c_str());
 *       return 1;
 *   }
 *
 *   for (const auto& row : result.rows) {
 *       printf("%s\n", row[0].c_str());
 *   }
 */

#pragma once

#include "vtable.hpp"
#include "functions.hpp"
#include "script.hpp"
#include "status.hpp"
#include <chrono>
#include <memory>
#include <utility>

namespace xsql {

// ============================================================================
// Query Result Types
// ============================================================================

struct Row {
    std::vector<std::string> values;

    const std::string& operator[](size_t i) const { return values[i]; }
    std::string& operator[](size_t i) { return values[i]; }
    size_t size() const { return values.size(); }
    bool empty() const { return values.empty(); }
};

struct QueryOptions {
    // Timeout for query execution in milliseconds.
    // 0 means disabled (legacy behavior).
    int timeout_ms = 0;

    // SQLite VM instructions between timeout checks.
    // Lower values improve responsiveness at the cost of overhead.
    int progress_steps = 1000;
};

struct Result {
    std::vector<std::string> columns;
    std::vector<Row> rows;
    std::string error;
    std::vector<std::string> warnings;
    bool timed_out = false;
    bool partial = false;
    int elapsed_ms = 0;

    bool ok() const { return error.empty(); }
    size_t size() const { return rows.size(); }
    bool empty() const { return rows.empty(); }

    const Row& operator[](size_t i) const { return rows[i]; }

    // Iterator support
    auto begin() { return rows.begin(); }
    auto end() { return rows.end(); }
    auto begin() const { return rows.begin(); }
    auto end() const { return rows.end(); }
};

// ============================================================================
// Database Wrapper
// ============================================================================

class Database {
public:
    Database() { open(":memory:"); }

    /**
     * Constructor with explicit path
     */
    explicit Database(const char* path) { open(path); }
    ~Database() { close(); }

    // Non-copyable
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    // Movable
    Database(Database&& other) noexcept
        : db_(other.db_), last_error_(std::move(other.last_error_)) {
        other.db_ = nullptr;
    }

    Database& operator=(Database&& other) noexcept {
        if (this != &other) {
            close();
            db_ = other.db_;
            last_error_ = std::move(other.last_error_);
            other.db_ = nullptr;
        }
        return *this;
    }

    // ========================================================================
    // Open/Close
    // ========================================================================

    bool open(const char* path = ":memory:") {
        close();
        int rc = sqlite3_open(path, &db_);
        if (!is_ok(rc)) {
            last_error_ = db_ ? sqlite3_errmsg(db_) : "Failed to allocate database";
            if (db_) {
                sqlite3_close(db_);
                db_ = nullptr;
            }
            return false;
        }
        last_error_.clear();
        return true;
    }

    void close() {
        if (db_) {
            sqlite3_close(db_);
            db_ = nullptr;
        }
    }

    bool is_open() const { return db_ != nullptr; }

    // ========================================================================
    // Table Registration
    // ========================================================================

    bool register_table(const VTableDef& def) {
        if (!db_) {
            last_error_ = "Database not open";
            return false;
        }
        return register_vtable(db_, def.name.c_str(), &def);
    }

    bool register_table(const char* module_name, const VTableDef* def) {
        if (!db_) {
            last_error_ = "Database not open";
            return false;
        }
        return register_vtable(db_, module_name, def);
    }

    bool create_table(const char* table_name, const char* module_name) {
        if (!db_) {
            last_error_ = "Database not open";
            return false;
        }
        return xsql::create_vtable(db_, table_name, module_name);
    }

    bool register_and_create_table(const VTableDef& def) {
        return register_table(def) &&
               create_table(def.name.c_str(), def.name.c_str());
    }

    /**
     * Register and create a virtual table with different table name
     */
    bool register_and_create_table(const VTableDef& def, const char* table_name) {
        return register_table(def) &&
               create_table(table_name, def.name.c_str());
    }

    /**
     * Register and create multiple virtual tables at once
     */
    template<typename... Defs>
    bool register_and_create_tables(Defs&... defs) {
        return (register_and_create_table(defs) && ...);
    }

    // ========================================================================
    // Table Registration - Cached Virtual Tables
    // ========================================================================

    template<typename RowData>
    bool register_cached_table(const CachedTableDef<RowData>& def) {
        if (!db_) {
            last_error_ = "Database not open";
            return false;
        }
        return register_cached_vtable(db_, def.name.c_str(), &def);
    }

    template<typename RowData>
    bool register_cached_table(const char* module_name, const CachedTableDef<RowData>* def) {
        if (!db_) {
            last_error_ = "Database not open";
            return false;
        }
        return register_cached_vtable(db_, module_name, def);
    }

    template<typename RowData>
    bool register_and_create_cached_table(const CachedTableDef<RowData>& def) {
        return register_cached_table(def) &&
               create_table(def.name.c_str(), def.name.c_str());
    }

    template<typename RowData>
    bool register_and_create_cached_table(const CachedTableDef<RowData>& def, const char* table_name) {
        return register_cached_table(def) &&
               create_table(table_name, def.name.c_str());
    }

    // ========================================================================
    // Table Registration - Generator Virtual Tables
    // ========================================================================

    template<typename RowData>
    bool register_generator_table(const GeneratorTableDef<RowData>& def) {
        if (!db_) {
            last_error_ = "Database not open";
            return false;
        }
        return register_generator_vtable(db_, def.name.c_str(), &def);
    }

    template<typename RowData>
    bool register_generator_table(const char* module_name, const GeneratorTableDef<RowData>* def) {
        if (!db_) {
            last_error_ = "Database not open";
            return false;
        }
        return register_generator_vtable(db_, module_name, def);
    }

    template<typename RowData>
    bool register_and_create_generator_table(const GeneratorTableDef<RowData>& def) {
        return register_generator_table(def) &&
               create_table(def.name.c_str(), def.name.c_str());
    }

    template<typename RowData>
    bool register_and_create_generator_table(const GeneratorTableDef<RowData>& def, const char* table_name) {
        return register_generator_table(def) &&
               create_table(table_name, def.name.c_str());
    }

    // ========================================================================
    // Function Registration
    // ========================================================================

    Status register_function(const char* name, int argc, SqlScalarFn fn) {
        if (!db_) {
            last_error_ = "Database not open";
            return Status::error;
        }
        return register_scalar_function(db_, name, argc, std::move(fn));
    }

    Status register_function(const char* name, int argc, ScalarFn fn) {
        if (!db_) {
            last_error_ = "Database not open";
            return Status::error;
        }
        return register_scalar_function(db_, name, argc, std::move(fn));
    }

    // ========================================================================
    // Query Execution
    // ========================================================================

    Result query(const char* sql) {
        return query(sql, QueryOptions{});
    }

    Result query(const char* sql, const QueryOptions& options) {
        Result result;

        if (!db_) {
            result.error = "Database not open";
            return result;
        }

        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
        if (!is_ok(rc)) {
            result.error = sqlite3_errmsg(db_);
            return result;
        }

        struct timeout_state_t {
            std::chrono::steady_clock::time_point started_at{};
            int timeout_ms = 0;
            bool timed_out = false;
        };

        struct progress_handler_t {
            static int callback(void* user_data) {
                auto* state = static_cast<timeout_state_t*>(user_data);
                if (!state || state->timeout_ms <= 0) {
                    return 0;
                }

                const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - state->started_at).count();
                if (elapsed_ms >= state->timeout_ms) {
                    state->timed_out = true;
                    return 1;  // Abort query with SQLITE_INTERRUPT
                }
                return 0;
            }
        };

        timeout_state_t timeout_state;
        const bool timeout_enabled = options.timeout_ms > 0;
        if (timeout_enabled) {
            timeout_state.started_at = std::chrono::steady_clock::now();
            timeout_state.timeout_ms = options.timeout_ms;
            const int progress_steps = options.progress_steps > 0 ? options.progress_steps : 1000;
            sqlite3_progress_handler(db_, progress_steps, &progress_handler_t::callback, &timeout_state);
        }

        const auto query_started_at = std::chrono::steady_clock::now();

        // Get column names
        int col_count = sqlite3_column_count(stmt);
        result.columns.reserve(col_count);
        for (int i = 0; i < col_count; ++i) {
            const char* name = sqlite3_column_name(stmt, i);
            result.columns.push_back(name ? name : "");
        }

        // Fetch rows
        while ((rc = sqlite3_step(stmt)) == to_sqlite_status(Status::row)) {
            Row row;
            row.values.reserve(col_count);
            for (int i = 0; i < col_count; ++i) {
                const char* text = reinterpret_cast<const char*>(
                    sqlite3_column_text(stmt, i));
                row.values.push_back(text ? text : "");
            }
            result.rows.push_back(std::move(row));
        }

        result.elapsed_ms = static_cast<int>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - query_started_at).count());

        if (timeout_enabled) {
            // Clear progress handler after this statement.
            sqlite3_progress_handler(db_, 0, nullptr, nullptr);
        }

        const bool timed_out = timeout_enabled && timeout_state.timed_out;
        if (timed_out) {
            result.timed_out = true;
            if (col_count > 0) {
                // SELECT-like statements return partial results collected so far.
                result.partial = true;
                result.warnings.push_back("query timed out; returning partial rows");
            } else {
                result.error = "Query timed out";
            }
        } else if (rc != to_sqlite_status(Status::done)) {
            result.error = sqlite3_errmsg(db_);
        }

        sqlite3_finalize(stmt);
        return result;
    }

    Result query(const std::string& sql) {
        return query(sql.c_str());
    }

    Result query(const std::string& sql, const QueryOptions& options) {
        return query(sql.c_str(), options);
    }

    /**
     * Get single value (first column of first row)
     */
    std::string scalar(const char* sql) {
        auto result = query(sql);
        if (result.ok() && !result.empty()) {
            return result[0][0];
        }
        return "";
    }

    std::string scalar(const std::string& sql) {
        return scalar(sql.c_str());
    }

    Status exec(const char* sql) {
        if (!db_) {
            last_error_ = "Database not open";
            return Status::error;
        }

        char* err = nullptr;
        int rc = sqlite3_exec(db_, sql, nullptr, nullptr, &err);
        if (err) {
            last_error_ = err;
            sqlite3_free(err);
        } else if (!is_ok(rc)) {
            last_error_ = sqlite3_errmsg(db_);
        } else {
            last_error_.clear();
        }
        return to_status(rc);
    }

    Status exec(const std::string& sql) {
        return exec(sql.c_str());
    }

    /**
     * Execute SQL with callback (for custom result handling)
     */
    int exec(const char* sql, int (*callback)(void*, int, char**, char**), void* data) {
        if (!db_) {
            last_error_ = "Database not open";
            return to_sqlite_status(Status::error);
        }

        char* err = nullptr;
        int rc = sqlite3_exec(db_, sql, callback, data, &err);
        if (err) {
            last_error_ = err;
            sqlite3_free(err);
        } else if (!is_ok(rc)) {
            last_error_ = sqlite3_errmsg(db_);
        } else {
            last_error_.clear();
        }
        return rc;
    }

    bool execute_script(const std::string& script,
                        std::vector<StatementResult>& results,
                        std::string& error) {
        if (!db_) {
            error = "Database not open";
            last_error_ = error;
            return false;
        }

        bool ok = xsql::execute_script(db_, script, results, error);
        if (ok) {
            last_error_.clear();
        } else {
            last_error_ = error;
        }
        return ok;
    }

    bool export_tables(const std::vector<std::string>& tables,
                       const std::string& output_path,
                       std::string& error) {
        if (!db_) {
            error = "Database not open";
            last_error_ = error;
            return false;
        }

        bool ok = xsql::export_tables(db_, tables, output_path, error);
        if (ok) {
            last_error_.clear();
        } else {
            last_error_ = error;
        }
        return ok;
    }

    // ========================================================================
    // Direct Access
    // ========================================================================

    sqlite3* handle() const { return db_; }
    const std::string& last_error() const { return last_error_; }

    // ========================================================================
    // Utility
    // ========================================================================

    int64_t last_insert_rowid() const {
        return db_ ? sqlite3_last_insert_rowid(db_) : 0;
    }

    int changes() const {
        return db_ ? sqlite3_changes(db_) : 0;
    }

private:
    sqlite3* db_ = nullptr;
    std::string last_error_;
};

} // namespace xsql
