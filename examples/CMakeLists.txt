# libxsql examples

# Basic read-only table
add_executable(example_basic_table basic_table.cpp)
target_link_libraries(example_basic_table PRIVATE xsql::xsql)

# Writable table with UPDATE/DELETE
add_executable(example_writable_table writable_table.cpp)
target_link_libraries(example_writable_table PRIVATE xsql::xsql)

# Cached table with constraint pushdown
add_executable(example_cached_table cached_table.cpp)
target_link_libraries(example_cached_table PRIVATE xsql::xsql)

# Repro: NOCHANGE bug with no_shared_cache + filter_eq + row_populator
add_executable(repro_nochange_bug repro_nochange_bug.cpp)
target_link_libraries(repro_nochange_bug PRIVATE xsql::xsql)

