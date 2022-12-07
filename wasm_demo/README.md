## POC WASM UDA

Datafusion has support for scalar and aggregate user defined functions.
This crate is implements the mean aggregate function in Rust as the guest langauge which is compiled to WASM.

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
    company_sensors> select wmean(usage_idle) from cpu;

The `wmean` function is backed by the WASM runtime.

Some thoughts:

* The current implementation is very slow because it compiles the WASM bytecode with each query. We need hooks in the IOx context creation to do this once on startup.
* Without specific tooling in place I have to choose the names of the exported functions carefully and with stutter in order to understand them on the Rust host side.
* The implementation lives in `query_functions/wasm.rs` which does some very naive reflection to determine the exported API from the WASM module.
* I had to hack in the copy of Arrow Arrays. The Arrow FFI functions assume that both sides have access to the same memory space which is not the case for WASM.

