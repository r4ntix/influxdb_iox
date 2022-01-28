This directory contains the major test suite for the IOx query engine

It is a mixture of tests defined in both rust code and in sql/expected files.

If at all possible please add new tests as SQL and not Rust source files (think of the compile time!)

## To add a new SQL / expected file

1. Create a SQL file in `cases/in/` following one of the existing files as a template
2. Re-generate cases.rs: `(cd generate && cargo run)`
