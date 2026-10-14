# Tracy profiling

## Profiling lane

Build with Tracy in every yams library (`yams_tracy_args`/`yams_tracy_deps` in
the root `meson.build`; clients are built with `TRACY_ON_DEMAND`):

    ./setup.sh Profiling --with-tests          # build/profiling
    meson compile -C build/profiling -j12 catch2_memory_sync yams-daemon

Capture (client listens on 8086 by default; if taken it falls back to the next
free port, so find it with `ss -ltnp | grep <pid>`):

    TRACY_NO_EXIT=1 build/profiling/tests/catch2_memory_sync &
    tracy-capture -o run.tracy -a 127.0.0.1 -p <port> -f
    tracy-csvexport -e run.tracy > zones.csv

Columns: `name,src_file,src_line,total_ns,total_perc,counts,mean_ns,min_ns,max_ns,std_ns`.
With `-e` (`--self`) the time columns hold self time instead of inclusive time;
export twice to get both.

Clients are `TRACY_ON_DEMAND`: zones fired before `tracy-capture` connects are
lost, so short tests need `TRACY_NO_EXIT=1` and a capture started while the
process is still alive, and the workload must run after the connect.

`tracy-capture` and `tracy-csvexport` must come from the same Tracy release as
the Conan client (`conan list "tracy/*"`); a mismatched protocol version is
rejected on connect. On grim they live in `/data/tracy-tools`, built with
`-DNO_FILESELECTOR=ON -DDOWNLOAD_CAPSTONE=OFF`.

With `enable-profiling=false` nothing changes: no args, no deps.
