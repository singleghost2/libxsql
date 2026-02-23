#pragma once

#include <sqlite3.h>

namespace xsql {

enum class Status : int {
    ok = SQLITE_OK,
    error = SQLITE_ERROR,
    internal = SQLITE_INTERNAL,
    perm = SQLITE_PERM,
    abort = SQLITE_ABORT,
    busy = SQLITE_BUSY,
    locked = SQLITE_LOCKED,
    no_memory = SQLITE_NOMEM,
    read_only = SQLITE_READONLY,
    interrupt = SQLITE_INTERRUPT,
    io_error = SQLITE_IOERR,
    corrupt = SQLITE_CORRUPT,
    not_found = SQLITE_NOTFOUND,
    full = SQLITE_FULL,
    cant_open = SQLITE_CANTOPEN,
    protocol = SQLITE_PROTOCOL,
    empty = SQLITE_EMPTY,
    schema = SQLITE_SCHEMA,
    too_big = SQLITE_TOOBIG,
    constraint = SQLITE_CONSTRAINT,
    mismatch = SQLITE_MISMATCH,
    misuse = SQLITE_MISUSE,
    no_lfs = SQLITE_NOLFS,
    auth = SQLITE_AUTH,
    format = SQLITE_FORMAT,
    range = SQLITE_RANGE,
    not_a_db = SQLITE_NOTADB,
    notice = SQLITE_NOTICE,
    warning = SQLITE_WARNING,
    row = SQLITE_ROW,
    done = SQLITE_DONE
};

constexpr int to_sqlite_status(Status status) {
    return static_cast<int>(status);
}

constexpr Status to_status(int sqlite_status) {
    return static_cast<Status>(sqlite_status);
}

constexpr bool is_ok(Status status) {
    return status == Status::ok;
}

constexpr bool is_ok(int sqlite_status) {
    return sqlite_status == to_sqlite_status(Status::ok);
}

constexpr bool is_row(int sqlite_status) {
    return sqlite_status == to_sqlite_status(Status::row);
}

constexpr bool is_done(int sqlite_status) {
    return sqlite_status == to_sqlite_status(Status::done);
}

} // namespace xsql
