## POC WASM UDF

Datafusion has support for scalar and aggregate user defined functions.
This crate is implements a multiply function in Rust which can be compiled to WASM and executed via a SQL query.

Try it:

    $ cargo build # Build IOx
    $ cd wasm_demo
    $ cargo build --target wasm32-unknown-unknown --release # Build demo code to wasm module
    $ cd ../
    $ ./target/debug/influxdb_iox run all-in-one --log-filter 'info,query_functions::wasm=debug'

In a new terminal

    $ ./target/debug/influxdb_iox -vv write company_sensors ../test_fixtures/lineproto/metrics.lp --host http://localhost:8080 #Load test data
    $ ./target/debug/influxdb_iox sql # Start SQL REPL
    > use company_sensors;
    company_sensors> select wasm_mul(usage_idle, -1.0) from cpu LIMIT 10;


The `wasm_mul` function is backed by the WASM runtime.

Experiment with the `lib.rs` implementation, add a new function, keep in mind using `f64` is recommended because I found bugs when datafusion attempted to coerce data types.

Some thoughts:

* The current implementation is very slow because it compiles the WASM bytecode with each query. We need hooks in the IOx context creation to do this once on startup.
* Only scalar functions are implemented currently. I have explored adding aggregate functions but still do not have it working. The missing piece is to be able to share an Arrow buffer with the WASM memory space.
* The implementation lives in `query_functions/wasm.rs`.
