# Simeon embedding backend glue. The actual static archive is produced by
# src/vector/meson.build (which also bundles the simeon core/SIMD TUs), so
# this subdir only declares sources/includes and exports a dependency that
# the vector module pulls in. Keeping the file here preserves directory
# ownership and stops src/vector from reaching into a sibling tree.

yams_embedding_simeon_srcs = [files('simeon_embedding_backend.cpp')]
yams_embedding_simeon_inc = include_directories('../../include', is_system: false)
