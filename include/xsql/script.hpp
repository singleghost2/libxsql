/**
 * xsql/script.hpp - SQL script execution and table export utilities
 *
 * Part of libxsql - a generic SQLite virtual table framework.
 *
 * Provides:
 *   - collect_statements: split a SQL script into individual statements
 *   - execute_script: run a multi-statement script, collecting results
 *   - export_tables: dump tables to a SQL file with schema + data
 *   - quote_identifier: safely quote a SQL identifier
 */

#pragma once

#include <sqlite3.h>
#include "status.hpp"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

namespace xsql {

struct StatementResult {
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;
};

namespace detail {

inline void skip_leading_ws(const char*& sql) {
    while (sql && *sql && std::isspace(static_cast<unsigned char>(*sql)))
        ++sql;
}

inline std::string trim_copy(const std::string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) start++;
    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) end--;
    return s.substr(start, end - start);
}

inline bool prepare_next(sqlite3* db,
                         const char*& sql,
                         const char*& tail_out,
                         sqlite3_stmt** stmt,
                         std::string& error) {
    *stmt = nullptr;
    tail_out = sql;
    if (!sql) return false;

    while (*sql) {
        skip_leading_ws(sql);
        if (*sql == '\0') return false;

        const char* tail = nullptr;
        int rc = sqlite3_prepare_v2(db, sql, -1, stmt, &tail);
        if (!xsql::is_ok(rc)) {
            error = sqlite3_errmsg(db);
            return false;
        }

        if (*stmt == nullptr) {
            if (tail == sql) ++sql;
            else sql = tail;
            continue;
        }

        tail_out = tail ? tail : sql;
        return true;
    }
    return false;
}

inline std::string escape_text(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 4);
    for (char c : value) {
        if (c == '\'') out += "''";
        else out += c;
    }
    return out;
}

inline std::string blob_to_hex(const void* data, int size) {
    auto* bytes = static_cast<const unsigned char*>(data);
    std::ostringstream oss;
    oss << "X'";
    oss << std::hex << std::uppercase << std::setfill('0');
    for (int i = 0; i < size; i++)
        oss << std::setw(2) << static_cast<int>(bytes[i]);
    oss << "'";
    return oss.str();
}

} // namespace detail

inline std::string quote_identifier(const std::string& name) {
    std::string out;
    out.reserve(name.size() + 2);
    out.push_back('"');
    for (char c : name) {
        if (c == '"') out.push_back('"');
        out.push_back(c);
    }
    out.push_back('"');
    return out;
}

inline bool collect_statements(sqlite3* /*db*/,
                               const std::string& script,
                               std::vector<std::string>& statements,
                               std::string& error) {
    (void)error;
    std::string current;
    for (char c : script) {
        current.push_back(c);
        if (sqlite3_complete(current.c_str())) {
            auto trimmed = detail::trim_copy(current);
            if (!trimmed.empty())
                statements.push_back(std::move(trimmed));
            current.clear();
        }
    }

    auto tail = detail::trim_copy(current);
    if (!tail.empty())
        statements.push_back(std::move(tail));

    return true;
}

inline bool execute_script(sqlite3* db,
                           const std::string& script,
                           std::vector<StatementResult>& results,
                           std::string& error) {
    const char* sql = script.c_str();
    while (true) {
        sqlite3_stmt* stmt = nullptr;
        const char* tail = nullptr;
        if (!detail::prepare_next(db, sql, tail, &stmt, error))
            return error.empty();

        if (!stmt) continue;

        int col_count = sqlite3_column_count(stmt);
        if (col_count > 0) {
            StatementResult res;
            res.columns.reserve(static_cast<size_t>(col_count));
            for (int i = 0; i < col_count; i++) {
                const char* name = sqlite3_column_name(stmt, i);
                res.columns.push_back(name ? name : "");
            }

            int rc = to_sqlite_status(Status::ok);
            while (xsql::is_row(rc = sqlite3_step(stmt))) {
                std::vector<std::string> row;
                row.reserve(static_cast<size_t>(col_count));
                for (int i = 0; i < col_count; i++) {
                    const unsigned char* txt = sqlite3_column_text(stmt, i);
                    if (!txt) row.emplace_back("NULL");
                    else row.emplace_back(reinterpret_cast<const char*>(txt));
                }
                res.rows.push_back(std::move(row));
            }

            if (!xsql::is_done(rc)) {
                error = sqlite3_errmsg(db);
                sqlite3_finalize(stmt);
                return false;
            }

            results.push_back(std::move(res));
        } else {
            int rc = to_sqlite_status(Status::ok);
            while (xsql::is_row(rc = sqlite3_step(stmt))) {}
            if (!xsql::is_done(rc)) {
                error = sqlite3_errmsg(db);
                sqlite3_finalize(stmt);
                return false;
            }
        }

        sqlite3_finalize(stmt);
        sql = tail;
    }
}

inline bool export_tables(sqlite3* db,
                          const std::vector<std::string>& requested_tables,
                          const std::string& output_path,
                          std::string& error) {
    std::vector<std::string> tables = requested_tables;

    if (tables.empty()) {
        const char* sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;";
        sqlite3_stmt* stmt = nullptr;
        if (!xsql::is_ok(sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr))) {
            error = sqlite3_errmsg(db);
            return false;
        }
        while (xsql::is_row(sqlite3_step(stmt))) {
            const unsigned char* name = sqlite3_column_text(stmt, 0);
            if (name)
                tables.emplace_back(reinterpret_cast<const char*>(name));
        }
        sqlite3_finalize(stmt);
    }

    std::ofstream out(output_path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        error = "Cannot open output file: " + output_path;
        return false;
    }

    out << "-- SQL Export\n";
    out << "-- Tables: " << tables.size() << "\n\n";

    for (const auto& table : tables) {
        const std::string quoted_table = quote_identifier(table);

        struct Column {
            std::string name;
            std::string type;
            bool notnull = false;
            bool pk = false;
            std::string dflt;
        };
        std::vector<Column> columns;

        std::string pragma = "PRAGMA table_info(" + quoted_table + ");";
        sqlite3_stmt* col_stmt = nullptr;
        if (!xsql::is_ok(sqlite3_prepare_v2(db, pragma.c_str(), -1, &col_stmt, nullptr))) {
            error = sqlite3_errmsg(db);
            return false;
        }

        while (xsql::is_row(sqlite3_step(col_stmt))) {
            Column col;
            col.name = reinterpret_cast<const char*>(sqlite3_column_text(col_stmt, 1));
            const unsigned char* type = sqlite3_column_text(col_stmt, 2);
            col.type = type ? reinterpret_cast<const char*>(type) : "";
            col.notnull = sqlite3_column_int(col_stmt, 3) != 0;
            const unsigned char* dflt = sqlite3_column_text(col_stmt, 4);
            col.dflt = dflt ? reinterpret_cast<const char*>(dflt) : "";
            col.pk = sqlite3_column_int(col_stmt, 5) != 0;
            columns.push_back(std::move(col));
        }
        sqlite3_finalize(col_stmt);

        if (columns.empty()) continue;

        out << "-- Table: " << table << "\n";
        out << "DROP TABLE IF EXISTS " << quoted_table << ";\n";
        out << "CREATE TABLE " << quoted_table << " (\n";
        for (size_t i = 0; i < columns.size(); i++) {
            const auto& col = columns[i];
            out << "    " << quote_identifier(col.name);
            if (!col.type.empty()) out << " " << col.type;
            if (col.pk) out << " PRIMARY KEY";
            if (col.notnull) out << " NOT NULL";
            if (!col.dflt.empty()) out << " DEFAULT " << col.dflt;
            if (i + 1 < columns.size()) out << ",";
            out << "\n";
        }
        out << ");\n\n";

        std::string select = "SELECT * FROM " + quoted_table + ";";
        sqlite3_stmt* data_stmt = nullptr;
        if (!xsql::is_ok(sqlite3_prepare_v2(db, select.c_str(), -1, &data_stmt, nullptr))) {
            error = sqlite3_errmsg(db);
            return false;
        }

        size_t row_count = 0;
        while (true) {
            int rc = sqlite3_step(data_stmt);
            if (xsql::is_done(rc)) break;
            if (!xsql::is_row(rc)) {
                error = sqlite3_errmsg(db);
                sqlite3_finalize(data_stmt);
                return false;
            }

            out << "INSERT INTO " << quoted_table << " VALUES (";
            int col_count = sqlite3_column_count(data_stmt);
            for (int i = 0; i < col_count; i++) {
                int type = sqlite3_column_type(data_stmt, i);
                switch (type) {
                    case SQLITE_NULL:
                        out << "NULL";
                        break;
                    case SQLITE_INTEGER:
                        out << sqlite3_column_int64(data_stmt, i);
                        break;
                    case SQLITE_FLOAT: {
                        std::ostringstream oss;
                        oss << std::setprecision(std::numeric_limits<double>::digits10 + 1)
                            << sqlite3_column_double(data_stmt, i);
                        out << oss.str();
                        break;
                    }
                    case SQLITE_BLOB: {
                        const void* blob = sqlite3_column_blob(data_stmt, i);
                        int size = sqlite3_column_bytes(data_stmt, i);
                        out << detail::blob_to_hex(blob, size);
                        break;
                    }
                    case SQLITE_TEXT:
                    default: {
                        const unsigned char* txt = sqlite3_column_text(data_stmt, i);
                        int size = sqlite3_column_bytes(data_stmt, i);
                        std::string value;
                        value.assign(reinterpret_cast<const char*>(txt),
                                     static_cast<size_t>(size));
                        out << "'" << detail::escape_text(value) << "'";
                        break;
                    }
                }
                if (i + 1 < col_count) out << ", ";
            }
            out << ");\n";
            row_count++;
        }

        sqlite3_finalize(data_stmt);
        out << "-- " << row_count << " rows exported\n\n";
    }

    return true;
}

} // namespace xsql
