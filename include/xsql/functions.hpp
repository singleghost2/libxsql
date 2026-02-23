/**
 * xsql/functions.hpp - SQL function registration helpers
 *
 * Part of libxsql - a generic SQLite virtual table framework.
 *
 * Provides utilities for registering custom SQL functions.
 *
 * Two APIs:
 *   1. Raw API: SqlScalarFn with sqlite3_context and sqlite3_value (legacy)
 *   2. Wrapped API: ScalarFn with FunctionContext and FunctionArg (preferred)
 *
 * The wrapped API hides raw sqlite3 types so tool code never needs <sqlite3.h>.
 */

#pragma once

#include <sqlite3.h>
#include "status.hpp"
#include <string>
#include <functional>
#include <cstdint>

namespace xsql {

// ============================================================================
// Wrapper Types (hide raw sqlite3 types from tool code)
// ============================================================================

/**
 * Wraps a sqlite3_value* for reading function arguments.
 *
 * Tool code uses FunctionArg instead of raw sqlite3_value*.
 */
class FunctionArg {
    sqlite3_value* val_;
public:
    explicit FunctionArg(sqlite3_value* v) : val_(v) {}

    int64_t as_int64() const { return sqlite3_value_int64(val_); }
    int as_int() const { return sqlite3_value_int(val_); }
    double as_double() const { return sqlite3_value_double(val_); }
    std::string as_text() const {
        const char* t = reinterpret_cast<const char*>(sqlite3_value_text(val_));
        return t ? t : "";
    }
    const char* as_c_str() const {
        return reinterpret_cast<const char*>(sqlite3_value_text(val_));
    }
    const void* as_blob() const { return sqlite3_value_blob(val_); }
    int bytes() const { return sqlite3_value_bytes(val_); }
    int type() const { return sqlite3_value_type(val_); }
    bool is_null() const { return sqlite3_value_type(val_) == SQLITE_NULL; }
    bool is_nochange() const { return sqlite3_value_nochange(val_) != 0; }

    // Escape hatch: get underlying sqlite3_value*
    sqlite3_value* raw() const { return val_; }
};

/**
 * Read-only wrapper for a row returned by an internal query.
 *
 * Tool code can inspect values without calling sqlite3_column_* directly.
 */
class QueryRow {
    sqlite3_stmt* stmt_;
public:
    explicit QueryRow(sqlite3_stmt* stmt) : stmt_(stmt) {}

    const char* text(int col) const {
        return reinterpret_cast<const char*>(sqlite3_column_text(stmt_, col));
    }
    int int_value(int col) const { return sqlite3_column_int(stmt_, col); }
    int64_t int64_value(int col) const { return sqlite3_column_int64(stmt_, col); }
    double double_value(int col) const { return sqlite3_column_double(stmt_, col); }
    bool is_null(int col) const { return sqlite3_column_type(stmt_, col) == SQLITE_NULL; }
};

/**
 * Wraps a sqlite3_context* for setting function results.
 *
 * Tool code uses FunctionContext instead of raw sqlite3_context*.
 */
class FunctionContext {
    sqlite3_context* ctx_;
public:
    explicit FunctionContext(sqlite3_context* c) : ctx_(c) {}

    void result_int(int val) { sqlite3_result_int(ctx_, val); }
    void result_int64(int64_t val) { sqlite3_result_int64(ctx_, val); }
    void result_text(const std::string& val) {
        sqlite3_result_text(ctx_, val.c_str(), -1, SQLITE_TRANSIENT);
    }
    void result_text(const char* val) {
        sqlite3_result_text(ctx_, val, -1, SQLITE_TRANSIENT);
    }
    void result_text_static(const char* val) {
        sqlite3_result_text(ctx_, val, -1, SQLITE_STATIC);
    }
    void result_double(double val) { sqlite3_result_double(ctx_, val); }
    void result_blob(const void* data, size_t len) {
        sqlite3_result_blob(ctx_, data, static_cast<int>(len), SQLITE_TRANSIENT);
    }
    void result_null() { sqlite3_result_null(ctx_); }
    void result_error(const std::string& msg) {
        sqlite3_result_error(ctx_, msg.c_str(), -1);
    }
    void result_error(const char* msg) {
        sqlite3_result_error(ctx_, msg, -1);
    }

    // Escape hatch: get database handle for schema queries
    sqlite3* db_handle() { return sqlite3_context_db_handle(ctx_); }

    /**
     * Execute a query and invoke callback for each row.
     * Returns false on SQL error and optionally writes the DB error message.
     */
    template<typename Fn>
    bool query_each(const std::string& sql, Fn&& fn, std::string* error = nullptr) {
        sqlite3* db = sqlite3_context_db_handle(ctx_);
        if (!db) {
            if (error) *error = "No database handle";
            return false;
        }

        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
        if (!xsql::is_ok(rc)) {
            if (error) *error = sqlite3_errmsg(db);
            return false;
        }

        while (xsql::is_row(rc = sqlite3_step(stmt))) {
            fn(QueryRow(stmt));
        }

        if (!xsql::is_done(rc)) {
            if (error) *error = sqlite3_errmsg(db);
            sqlite3_finalize(stmt);
            return false;
        }

        sqlite3_finalize(stmt);
        return true;
    }

    std::string db_error() const {
        sqlite3* db = sqlite3_context_db_handle(ctx_);
        return db ? std::string(sqlite3_errmsg(db)) : std::string("No database handle");
    }

    // Escape hatch: get underlying sqlite3_context*
    sqlite3_context* raw() const { return ctx_; }
};

namespace detail {

/// Convert sqlite3_value** to FunctionArg*, invoke callback.
/// FunctionArg is a trivial wrapper; this avoids copies via reinterpret_cast
/// when the layout is compatible, and falls back to a small-buffer otherwise.
template<typename Fn>
inline void with_args(int argc, sqlite3_value** argv, Fn&& fn) {
    static_assert(sizeof(FunctionArg) == sizeof(sqlite3_value*),
                  "FunctionArg must be pointer-sized for reinterpret_cast");
    // FunctionArg is a standard-layout class holding a single sqlite3_value*.
    // reinterpret_cast from sqlite3_value** to FunctionArg* is valid here.
    fn(reinterpret_cast<FunctionArg*>(argv));
}

} // namespace detail

// ============================================================================
// SQL Function Types
// ============================================================================

/// Legacy raw callback (requires #include <sqlite3.h> in caller)
using SqlScalarFn = std::function<void(sqlite3_context*, int argc, sqlite3_value**)>;

/// Wrapped callback using FunctionContext/FunctionArg
using ScalarFn = std::function<void(FunctionContext& ctx, int argc, FunctionArg* argv)>;

// ============================================================================
// Result Helpers (legacy free functions)
// ============================================================================

inline void result_int64(sqlite3_context* ctx, int64_t value) {
    sqlite3_result_int64(ctx, value);
}

inline void result_int(sqlite3_context* ctx, int value) {
    sqlite3_result_int(ctx, value);
}

inline void result_double(sqlite3_context* ctx, double value) {
    sqlite3_result_double(ctx, value);
}

inline void result_text(sqlite3_context* ctx, const std::string& value) {
    sqlite3_result_text(ctx, value.c_str(), -1, SQLITE_TRANSIENT);
}

inline void result_text(sqlite3_context* ctx, const char* value) {
    sqlite3_result_text(ctx, value, -1, SQLITE_TRANSIENT);
}

inline void result_blob(sqlite3_context* ctx, const void* data, size_t size) {
    sqlite3_result_blob(ctx, data, static_cast<int>(size), SQLITE_TRANSIENT);
}

inline void result_null(sqlite3_context* ctx) {
    sqlite3_result_null(ctx);
}

inline void result_error(sqlite3_context* ctx, const std::string& msg) {
    sqlite3_result_error(ctx, msg.c_str(), -1);
}

// ============================================================================
// Argument Helpers (legacy free functions)
// ============================================================================

inline int64_t arg_int64(sqlite3_value* val) {
    return sqlite3_value_int64(val);
}

inline int arg_int(sqlite3_value* val) {
    return sqlite3_value_int(val);
}

inline double arg_double(sqlite3_value* val) {
    return sqlite3_value_double(val);
}

inline std::string arg_text(sqlite3_value* val) {
    const char* text = reinterpret_cast<const char*>(sqlite3_value_text(val));
    return text ? text : "";
}

inline const void* arg_blob(sqlite3_value* val) {
    return sqlite3_value_blob(val);
}

inline int arg_bytes(sqlite3_value* val) {
    return sqlite3_value_bytes(val);
}

inline int arg_type(sqlite3_value* val) {
    return sqlite3_value_type(val);
}

inline bool arg_is_null(sqlite3_value* val) {
    return sqlite3_value_type(val) == SQLITE_NULL;
}

// ============================================================================
// Function Registration
// ============================================================================

namespace detail {

// Wrapper for legacy raw callbacks
struct FunctionWrapper {
    SqlScalarFn fn;
};

inline void scalar_callback(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    auto* wrapper = static_cast<FunctionWrapper*>(sqlite3_user_data(ctx));
    if (wrapper && wrapper->fn) {
        wrapper->fn(ctx, argc, argv);
    }
}

// Wrapper for new wrapped callbacks
struct ScalarFnWrapper {
    ScalarFn fn;
};

inline void scalar_fn_callback(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    auto* wrapper = static_cast<ScalarFnWrapper*>(sqlite3_user_data(ctx));
    if (wrapper && wrapper->fn) {
        FunctionContext fctx(ctx);
        xsql::detail::with_args(argc, argv, [&](FunctionArg* args) {
            wrapper->fn(fctx, argc, args);
        });
    }
}

inline void destroy_wrapper(void* ptr) {
    delete static_cast<FunctionWrapper*>(ptr);
}

inline void destroy_scalar_fn_wrapper(void* ptr) {
    delete static_cast<ScalarFnWrapper*>(ptr);
}

} // namespace detail

/// Register a raw SQL function (legacy, requires sqlite3.h)
inline Status register_scalar_function(
    sqlite3* db,
    const char* name,
    int argc,
    SqlScalarFn fn,
    int flags = SQLITE_UTF8 | SQLITE_DETERMINISTIC
) {
    auto* wrapper = new detail::FunctionWrapper{std::move(fn)};
    int rc = sqlite3_create_function_v2(
        db,
        name,
        argc,
        flags,
        wrapper,
        detail::scalar_callback,
        nullptr,
        nullptr,
        detail::destroy_wrapper
    );
    if (!xsql::is_ok(rc)) {
        delete wrapper;
    }
    return to_status(rc);
}

/// Register a wrapped SQL function using ScalarFn
inline Status register_scalar_function(
    sqlite3* db,
    const char* name,
    int argc,
    ScalarFn fn,
    int flags = SQLITE_UTF8 | SQLITE_DETERMINISTIC
) {
    auto* wrapper = new detail::ScalarFnWrapper{std::move(fn)};
    int rc = sqlite3_create_function_v2(
        db,
        name,
        argc,
        flags,
        wrapper,
        detail::scalar_fn_callback,
        nullptr,
        nullptr,
        detail::destroy_scalar_fn_wrapper
    );
    if (!xsql::is_ok(rc)) {
        delete wrapper;
    }
    return to_status(rc);
}

} // namespace xsql
